import logging
import azure.functions as func

@app.route(route="upload_Stock_Take_File", auth_level=func.AuthLevel.FUNCTION)
def upload_Stock_Take_File(req: func.HttpRequest) -> func.HttpResponse:

    return func.HttpResponse(
        "Function reached",
        status_code=200)


import json
import urllib.parse
import time

import pandas as pd
import requests
from sqlalchemy import create_engine
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


# ── Credentials (already defined in your environment) ─────────────────────────
# TOKEN_URL, CLIENT_ID, CLIENT_SECRET, GRAPHQL_URL
# server, database, username, password




TOKEN_URL = f"https://{SHOP}.myshopify.com/admin/oauth/access_token"
GRAPHQL_URL = f"https://{SHOP}.myshopify.com/admin/api/2025-01/graphql.json"
SHOP_URL = f"https://{SHOP}.myshopify.com"


# Key Vault
vault_url = "https://banner-key-vault.vault.azure.net/"

# Authenticate using Azure CLI login
credential = DefaultAzureCredential()

# Connect to Key Vault
client = SecretClient(
    vault_url=vault_url,
    credential=credential
)


# Connection details
server = "ban-powbi-sql-01.database.windows.net"
database = "banner-platform"
username = client.get_secret(
    "ban-powbi-sql-01-username"
).value
password = client.get_secret(
    "ban-powbi-sql-01-password"
).value


####################### Shopify Credentials
SHOP = "0ebxbs-yv"
CLIENT_ID = client.get_secret(
    "tc_client_id"
).value
CLIENT_SECRET = client.get_secret(
    "tc_client_secret"
).value


TOKEN_URL = f"https://{SHOP}.myshopify.com/admin/oauth/access_token"
GRAPHQL_URL = f"https://{SHOP}.myshopify.com/admin/api/2025-01/graphql.json"
SHOP_URL = f"https://{SHOP}.myshopify.com"

"""
shopify_datalake_bulk_upload.py
─────────────────────────────────
Simple, low-maintenance ingestion: pulls raw JSON for each Shopify resource
via the REST Admin API (no field-by-field GraphQL selection needed) and
loads each resource as its own table into the data lake.

Each row = one record, with a single `raw_json` column containing the
full untouched JSON for that record, plus a couple of generic top-level
columns (id, created_at, updated_at) pulled out for convenience.

This is intentionally "dumb" — it grabs everything Shopify gives back.
You can pick apart / reshape individual fields later once you know what
you actually need.
"""


# ── Credentials (already defined in your environment) ─────────────────────────
# TOKEN_URL, CLIENT_ID, CLIENT_SECRET
# SHOP_URL          e.g. "https://your-store.myshopify.com"
# server, database, username, password


# ══════════════════════════════════════════════════════════════════════════════
#  Auth
# ══════════════════════════════════════════════════════════════════════════════
def get_access_token():
    response = requests.post(
        TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        timeout=30,
    )
    if not response.ok:
        raise Exception(f"Token request failed: {response.status_code} - {response.text}")
    return response.json()["access_token"]


# ══════════════════════════════════════════════════════════════════════════════
#  REST resources to pull
#  table_name : (endpoint_path, json_key_in_response)
# ══════════════════════════════════════════════════════════════════════════════

RESOURCES = {
    "Orders":               ("orders.json", "orders", {"status": "any"}),
    "Customers":             ("customers.json", "customers", {}),
    "Products":              ("products.json", "products", {}),
    "ProductVariants":       (None, None, None),  # pulled from Products, skip standalone
    "Collections":           ("custom_collections.json", "custom_collections", {}),
    "SmartCollections":      ("smart_collections.json", "smart_collections", {}),
    "InventoryLevels":       ("inventory_levels.json", "inventory_levels", {}),
    "InventoryItems":        ("inventory_items.json", "inventory_items", {}),
    "Locations":             ("locations.json", "locations", {}),
    "PriceRules":            ("price_rules.json", "price_rules", {}),
    "GiftCards":             ("gift_cards.json", "gift_cards", {}),
    "Discounts":             ("discount_codes.json", "discount_codes", {}),
    "Fulfillments":          ("fulfillments.json", "fulfillments", {}),
    "Refunds":               (None, None, None),  # nested in orders, see below
    "Companies":             ("companies.json", "companies", {}),
    "DraftOrders":           ("draft_orders.json", "draft_orders", {}),
    "Transactions":          (None, None, None),  # nested in orders, see below
    "ShippingZones":         ("shipping_zones.json", "shipping_zones", {}),
    "Publications":          ("publications.json", "publications", {}),
}

PAGE_LIMIT = 250


# ══════════════════════════════════════════════════════════════════════════════
#  REST fetch helpers
# ══════════════════════════════════════════════════════════════════════════════

def rest_get(access_token, path, params=None):
    """Single REST GET, returns (json_body, next_page_info_or_None)."""
    url = f"{SHOP_URL}/admin/api/2024-10/{path}"
    response = requests.get(
        url,
        headers={
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        },
        params=params or {},
        timeout=60,
    )
    if not response.ok:
        raise Exception(f"REST request failed [{path}]: {response.status_code} - {response.text}")

    # Shopify rate limiting: back off if close to the limit
    call_limit = response.headers.get("X-Shopify-Shop-Api-Call-Limit")
    if call_limit:
        used, total = map(int, call_limit.split("/"))
        if used / total > 0.8:
            time.sleep(1)

    # Cursor-based pagination via Link header
    next_page_info = None
    link_header = response.headers.get("Link", "")
    if 'rel="next"' in link_header:
        for part in link_header.split(","):
            if 'rel="next"' in part:
                url_part = part.split(";")[0].strip().strip("<>")
                qs = urllib.parse.urlparse(url_part).query
                next_page_info = urllib.parse.parse_qs(qs).get("page_info", [None])[0]

    return response.json(), next_page_info


def fetch_all(access_token, path, json_key, extra_params=None):
    """Paginate through a REST endpoint, return list of raw dicts."""
    records = []
    params = {"limit": PAGE_LIMIT, **(extra_params or {})}
    page_info = None

    while True:
        call_params = dict(params)
        if page_info:
            # Once paginating, Shopify only accepts page_info + limit
            call_params = {"limit": PAGE_LIMIT, "page_info": page_info}

        body, next_page_info = rest_get(access_token, path, call_params)
        records.extend(body.get(json_key, []))

        if not next_page_info:
            break
        page_info = next_page_info

    return records


# ══════════════════════════════════════════════════════════════════════════════
#  Flatten raw record → DataFrame row (id / timestamps pulled out, rest as JSON)
# ══════════════════════════════════════════════════════════════════════════════

def record_to_row(record):
    return {
        "raw_json": json.dumps(record, ensure_ascii=False),
    }


def records_to_df(records):
    return pd.DataFrame([record_to_row(r) for r in records])


# ══════════════════════════════════════════════════════════════════════════════
#  Data lake helpers
# ══════════════════════════════════════════════════════════════════════════════

def get_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password}"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


def load_table(engine, table_name, df):
    df.to_sql(
        f"TC_{table_name}",
        engine,
        schema="raw_total_clothing",
        if_exists="replace",
        index=False,
    )
    print(f"  ✅  raw_total_clothing.TC_{table_name}  →  {len(df)} rows")


# ══════════════════════════════════════════════════════════════════════════════
#  Nested extraction: line items, refunds, transactions, fulfillments
#  pulled straight out of the Orders payload (no extra API calls needed)
# ══════════════════════════════════════════════════════════════════════════════

def explode_nested(orders, key):
    """Pull a nested list field out of each order, tag with parent order_id."""
    rows = []
    for order in orders:
        for item in order.get(key, []) or []:
            row = record_to_row(item)
            row["order_id"] = order.get("id")
            rows.append(row)
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["raw_json"])


# ══════════════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("🔑 Obtaining access token …")
    token = get_access_token()
    engine = get_engine()

    orders_cache = None  # so we don't re-fetch for nested resources

    for table_name, (path, json_key, extra_params) in RESOURCES.items():

        if path is None:
            continue  # handled separately below (nested resources)

        print(f"📥  Fetching {table_name} …")
        try:
            records = fetch_all(token, path, json_key, extra_params)
            df = records_to_df(records)
            load_table(engine, table_name, df)

            if table_name == "Orders":
                orders_cache = records  # reuse for LineItems/Refunds/Transactions

        except Exception as e:
            print(f"  ⚠️  {table_name} skipped — {e}")

    # ── Nested resources pulled out of Orders payload ───────────────────────
    if orders_cache:
        for table_name, key in [
            ("LineItems", "line_items"),
            ("Refunds", "refunds"),
            ("Fulfillments", "fulfillments"),
        ]:
            print(f"📥  Extracting {table_name} from Orders …")
            try:
                df = explode_nested(orders_cache, key)
                load_table(engine, table_name, df)
            except Exception as e:
                print(f"  ⚠️  {table_name} skipped — {e}")

            # Transactions need a separate per-order endpoint call
            print("📥  Fetching Transactions (per order) …")
            try:
                tx_rows = []
                for order in orders_cache:
                    order_id = order.get("id")
                    body, _ = rest_get(token, f"orders/{order_id}/transactions.json")

                    for tx in body.get("transactions", []):
                        tx_rows.append(record_to_row(tx))

                df = pd.DataFrame(tx_rows) if tx_rows else pd.DataFrame(
                    columns=["raw_json"]
                )

                load_table(engine, "Transactions", df)

            except Exception as e:
                print(f"  ⚠️  Transactions skipped — {e}")

        print("\n✅  All done.")


if __name__ == "__main__":
    main()
