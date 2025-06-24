#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopify-synk som tillåter negativa lager
och klarar 'None' i available-fältet.
"""

##############################################################################
# Imports
##############################################################################

import os, sys, time, json, psycopg2, requests
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

##############################################################################
# Konfig
##############################################################################

API_VERSION          = "2023-07"
SHOP_DOMAIN_1        = os.getenv("SHOP_DOMAIN_1")        or "first-shop.myshopify.com"
SHOPIFY_ACCESS_TOKEN_1 = os.getenv("SHOPIFY_ACCESS_TOKEN_1") or "token-shop-1"
SHOP_DOMAIN_2        = os.getenv("SHOP_DOMAIN_2")        or "second-shop.myshopify.com"
SHOPIFY_ACCESS_TOKEN_2 = os.getenv("SHOPIFY_ACCESS_TOKEN_2") or "token-shop-2"
DATABASE_URL         = os.getenv("DATABASE_URL")         or "postgres://..."
LOOKBACK_HOURS       = int(os.getenv("SYNC_LOOKBACK_HOURS", "24"))

##############################################################################
# DB-hjälp
##############################################################################

def create_table_if_not_exists():
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_orders (
                order_id TEXT PRIMARY KEY,
                processed_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """)

def load_processed_orders() -> set[str]:
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute("SELECT order_id FROM processed_orders;")
        return {r[0] for r in cur.fetchall()}

def save_processed_orders(order_ids: list[str]) -> None:
    if not order_ids:
        return
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO processed_orders (order_id) VALUES (%s) ON CONFLICT DO NOTHING;",
            [(oid,) for oid in order_ids],
        )

##############################################################################
# Shopify-wrapper
##############################################################################

def shopify_request(
    shop: str,
    token: str,
    method: str,
    path: str,
    params: Optional[dict] = None,
    payload: Optional[dict] = None,
) -> requests.Response:
    url = f"https://{shop}/admin/api/{API_VERSION}{path}"
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type":          "application/json",
        "Accept":                "application/json",
    }
    while True:
        resp = requests.request(method, url, headers=headers, params=params, json=payload)
        if resp.status_code == 429:
            time.sleep(int(resp.headers.get("Retry-After", 2)))
            continue
        if resp.status_code >= 400:
            raise RuntimeError(f"{shop} {method} {path} → {resp.status_code}: {resp.text}")
        return resp

##############################################################################
# Variant- & lager-hjälp
##############################################################################

def get_primary_location_id(shop: str, token: str) -> int:
    resp = shopify_request(shop, token, "GET", "/locations.json")
    for loc in resp.json()["locations"]:
        if loc.get("primary"):
            return loc["id"]
    return resp.json()["locations"][0]["id"]

VariantInfo = Tuple[int, int]  # (inventory_item_id, variant_id)

def fetch_variants_by_sku(shop: str, token: str) -> Dict[str, VariantInfo]:
    out: Dict[str, VariantInfo] = {}
    path = "/products.json"
    params = {"limit": 250, "fields": "variants"}
    while True:
        resp = shopify_request(shop, token, "GET", path, params=params)
        for p in resp.json()["products"]:
            for v in p["variants"]:
                sku = (v["sku"] or "").strip()
                if sku:
                    out[sku] = (v["inventory_item_id"], v["id"])
        link = resp.headers.get("Link")
        if link and 'rel="next"' in link:
            path = link.split(";")[0].strip("<>").split(f"{API_VERSION}")[1]
            params = {}
        else:
            break
    return out

def fetch_inventory_levels(
    shop: str,
    token: str,
    location_id: int,
    variants_map: Dict[str, VariantInfo],
) -> Dict[str, int]:
    """
    Returnerar dict SKU -> available (int).
    Om available är None returneras 0.
    """
    result: Dict[str, int] = {}
    ids = [str(iid) for iid, _ in variants_map.values()]
    for i in range(0, len(ids), 50):
        chunk = ",".join(ids[i : i + 50])
        resp = shopify_request(
            shop, token, "GET", "/inventory_levels.json",
            params={"inventory_item_ids": chunk, "location_ids": location_id},
        )
        for lvl in resp.json()["inventory_levels"]:
            qty = lvl.get("available")
            inv_item_id = lvl["inventory_item_id"]
            for sku, (iid, _) in variants_map.items():
                if iid == inv_item_id:
                    result[sku] = int(qty) if qty is not None else 0
                    break
    return result

##############################################################################
# Tracking & location-säkring
##############################################################################

def ensure_variant_is_trackable(shop: str, token: str, variant_id: int):
    v = shopify_request(shop, token, "GET", f"/variants/{variant_id}.json").json()["variant"]
    if v["inventory_management"] == "shopify":
        return
    shopify_request(
        shop, token, "PUT", f"/variants/{variant_id}.json",
        payload={"variant": {"id": variant_id, "inventory_management": "shopify"}},
    )
    print(f"✅ Track quantity ON för variant {variant_id} i {shop}")

def connect_if_needed(shop: str, token: str, inv_item_id: int, location_id: int):
    try:
        shopify_request(
            shop, token, "POST", "/inventory_levels/connect.json",
            payload={"location_id": location_id, "inventory_item_id": inv_item_id},
        )
        print(f"🔗 inventory_item {inv_item_id} kopplad till location {location_id} ({shop})")
    except RuntimeError as e:
        if "422" not in str(e):  # redan kopplad
            raise

##############################################################################
# Lager-operationer
##############################################################################

def adjust_inventory(shop: str, token: str, inv_item_id: int, location_id: int, diff: int):
    if diff == 0:
        return
    payload = {
        "location_id":          location_id,
        "inventory_item_id":    inv_item_id,
        "available_adjustment": diff,
    }
    shopify_request(shop, token, "POST", "/inventory_levels/adjust.json", payload=payload)

def force_set_inventory(shop: str, token: str, inv_item_id: int, location_id: int, qty: int):
    payload = {
        "location_id":       location_id,
        "inventory_item_id": inv_item_id,
        "available":         qty,
    }
    shopify_request(shop, token, "POST", "/inventory_levels/set.json", payload=payload)

##############################################################################
# Orders
##############################################################################

def fetch_new_orders(shop: str, token: str, since: datetime) -> List[dict]:
    orders: List[dict] = []
    path = "/orders.json"
    params = {
        "status": "any",
        "limit": 250,
        "created_at_min": since.isoformat(),
        "fields": "id,line_items(id,sku,quantity)",
    }
    while True:
        resp = shopify_request(shop, token, "GET", path, params=params)
        orders.extend(resp.json()["orders"])
        link = resp.headers.get("Link")
        if link and 'rel="next"' in link:
            path = link.split(";")[0].strip("<>").split(f"{API_VERSION}")[1]
            params = {}
        else:
            break
    return orders

def process_orders_from_secondary(
    secondary_shop: str,
    secondary_token: str,
    orders: List[dict],
    processed: set[str],
    prefix: str,
    master_shop: str,
    master_token: str,
    master_loc: int,
    master_variants: Dict[str, VariantInfo],
) -> List[str]:
    new_ids: List[str] = []
    for o in orders:
        uid = f"{prefix}{o['id']}"
        if uid in processed:
            continue
        for li in o["line_items"]:
            sku = (li.get("sku") or "").strip()
            qty = int(li["quantity"])
            if not sku or sku not in master_variants:
                continue
            inv_item_id, _ = master_variants[sku]
            adjust_inventory(master_shop, master_token, inv_item_id, master_loc, -qty)
        new_ids.append(uid)
    return new_ids

##############################################################################
# Synk-funktion
##############################################################################

def sync_master_to_secondary(
    master_shop: str,
    master_token: str,
    master_loc: int,
    master_qty: Dict[str, int],
    secondary_shop: str,
    secondary_token: str,
    secondary_loc: int,
    variants_secondary: Dict[str, VariantInfo],
):
    # Läs sekundär-saldo en gång
    sec_qty = fetch_inventory_levels(secondary_shop, secondary_token, secondary_loc, variants_secondary)

    updates = 0
    for sku, master_val in master_qty.items():
        if sku not in variants_secondary:
            continue

        master_val = master_val if master_val is not None else 0
        current_sec = sec_qty.get(sku, 0)
        current_sec = current_sec if current_sec is not None else 0
        diff = master_val - current_sec
        if diff == 0:
            continue

        inv_item_id, variant_id = variants_secondary[sku]

        ensure_variant_is_trackable(secondary_shop, secondary_token, variant_id)
        connect_if_needed(secondary_shop, secondary_token, inv_item_id, secondary_loc)

        try:
            adjust_inventory(secondary_shop, secondary_token, inv_item_id, secondary_loc, diff)
        except RuntimeError as e:
            print(f"⚠️ adjust misslyckades för SKU {sku} ({e}); fallback set→{master_val}")
            force_set_inventory(secondary_shop, secondary_token, inv_item_id, secondary_loc, master_val)

        updates += 1

    print(f"Lager uppdaterat i {secondary_shop} för {updates} SKU:er.")

##############################################################################
# MAIN
##############################################################################

def main():
    print("=== Shopify-synk (negativa saldo + None-skydd) startar ===")

    create_table_if_not_exists()
    processed = load_processed_orders()
    since     = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)

    # Master-data
    loc1      = get_primary_location_id(SHOP_DOMAIN_1, SHOPIFY_ACCESS_TOKEN_1)
    variants1 = fetch_variants_by_sku(SHOP_DOMAIN_1, SHOPIFY_ACCESS_TOKEN_1)

    loc2      = get_primary_location_id(SHOP_DOMAIN_2, SHOPIFY_ACCESS_TOKEN_2)
    variants2 = fetch_variants_by_sku(SHOP_DOMAIN_2, SHOPIFY_ACCESS_TOKEN_2)

    # Ordrar
    ord1 = fetch_new_orders(SHOP_DOMAIN_1, SHOPIFY_ACCESS_TOKEN_1, since)
    ord2 = fetch_new_orders(SHOP_DOMAIN_2, SHOPIFY_ACCESS_TOKEN_2, since)

    # Processera
    new_ids_1 = [str(o["id"]) for o in ord1 if str(o["id"]) not in processed]

    new_ids_2 = process_orders_from_secondary(
        secondary_shop  = SHOP_DOMAIN_2,
        secondary_token = SHOPIFY_ACCESS_TOKEN_2,
        orders          = ord2,
        processed       = processed,
        prefix          = f"{SHOP_DOMAIN_2}_",
        master_shop     = SHOP_DOMAIN_1,
        master_token    = SHOPIFY_ACCESS_TOKEN_1,
        master_loc      = loc1,
        master_variants = variants1,
    )

    save_processed_orders(new_ids_1 + new_ids_2)
    print(f"Nya order-ID sparade: {len(new_ids_1) + len(new_ids_2)} st")

    # Synka differenser
    master_qty = fetch_inventory_levels(SHOP_DOMAIN_1, SHOPIFY_ACCESS_TOKEN_1, loc1, variants1)
    sync_master_to_secondary(
        SHOP_DOMAIN_1, SHOPIFY_ACCESS_TOKEN_1, loc1, master_qty,
        SHOP_DOMAIN_2, SHOPIFY_ACCESS_TOKEN_2, loc2, variants2,
    )

    print("=== Synk klar ===")

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print("❌ Fel i synken:", err)
        sys.exit(1)
