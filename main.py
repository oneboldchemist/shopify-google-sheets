#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopify-lager-synk (v2.2)
-------------------------
* Master-butik = butik 1
* Negativa lager tillåts
* Detaljerad loggning av orderrader & synk
* Guard mot saknad 'line_items'
"""

##############################################################################
# Imports & konfiguration
##############################################################################

import os, sys, time, json, psycopg2, requests
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

API_VERSION              = "2023-07"
SHOP_DOMAIN_1            = os.getenv("SHOP_DOMAIN_1")            or "first-shop.myshopify.com"
SHOPIFY_ACCESS_TOKEN_1   = os.getenv("SHOPIFY_ACCESS_TOKEN_1")   or "token-shop-1"
SHOP_DOMAIN_2            = os.getenv("SHOP_DOMAIN_2")            or "second-shop.myshopify.com"
SHOPIFY_ACCESS_TOKEN_2   = os.getenv("SHOPIFY_ACCESS_TOKEN_2")   or "token-shop-2"
DATABASE_URL             = os.getenv("DATABASE_URL")             or "postgres://..."
LOOKBACK_HOURS           = int(os.getenv("SYNC_LOOKBACK_HOURS", "24"))

##############################################################################
# Databas
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
    if not order_ids: return
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
# Variant & lager-hjälp
##############################################################################

VariantInfo = Tuple[int, int]  # (inventory_item_id, variant_id)

def get_primary_location_id(shop: str, token: str) -> int:
    resp = shopify_request(shop, token, "GET", "/locations.json")
    for loc in resp.json()["locations"]:
        if loc.get("primary"): return loc["id"]
    return resp.json()["locations"][0]["id"]

def fetch_variants_by_sku(shop: str, token: str) -> Dict[str, VariantInfo]:
    variants: Dict[str, VariantInfo] = {}
    path   = "/products.json"
    params = {"limit": 250, "fields": "variants"}
    while True:
        resp = shopify_request(shop, token, "GET", path, params=params)
        for product in resp.json()["products"]:
            for v in product["variants"]:
                sku = (v["sku"] or "").strip()
                if sku:
                    variants[sku] = (v["inventory_item_id"], v["id"])
        link = resp.headers.get("Link")
        if link and 'rel="next"' in link:
            path = link.split(";")[0].strip("<>").split(f"{API_VERSION}")[1]
            params = {}
        else:
            break
    return variants

def fetch_inventory_levels(
    shop: str,
    token: str,
    location_id: int,
    variants_map: Dict[str, VariantInfo],
) -> Dict[str, int]:
    result: Dict[str, int] = {}
    ids = [str(iid) for iid, _ in variants_map.values()]
    for i in range(0, len(ids), 50):
        chunk = ",".join(ids[i:i+50])
        levels = shopify_request(
            shop, token, "GET", "/inventory_levels.json",
            params={"inventory_item_ids": chunk, "location_ids": location_id},
        ).json()["inventory_levels"]
        for lvl in levels:
            qty = lvl.get("available")
            inv_item_id = lvl["inventory_item_id"]
            for sku, (iid, _) in variants_map.items():
                if iid == inv_item_id:
                    result[sku] = int(qty) if qty is not None else 0
                    break
    return result

##############################################################################
# Tracking & connect
##############################################################################

def ensure_trackable(shop: str, token: str, variant_id: int):
    v = shopify_request(shop, token, "GET", f"/variants/{variant_id}.json").json()["variant"]
    if v["inventory_management"] == "shopify": return
    shopify_request(
        shop, token, "PUT", f"/variants/{variant_id}.json",
        payload={"variant": {"id": variant_id, "inventory_management": "shopify"}},
    )
    print(f"✅ Track ON ➜ variant {variant_id} ({shop})")

def connect_if_needed(shop: str, token: str, inv_item_id: int, loc_id: int):
    try:
        shopify_request(
            shop, token, "POST", "/inventory_levels/connect.json",
            payload={"location_id": loc_id, "inventory_item_id": inv_item_id},
        )
        print(f"🔗 Connected inventory_item {inv_item_id} → location {loc_id} ({shop})")
    except RuntimeError as e:
        if "422" not in str(e): raise  # redan kopplad är OK

##############################################################################
# Lager-justeringar
##############################################################################

def adjust_inventory(shop: str, token: str, inv_item_id: int, loc_id: int, diff: int, sku: str):
    if diff == 0: return
    shopify_request(
        shop, token, "POST", "/inventory_levels/adjust.json",
        payload={"location_id": loc_id, "inventory_item_id": inv_item_id, "available_adjustment": diff},
    )
    print(f"  🔧 adjust {diff:+}  (SKU {sku}, shop {shop})")

def force_set_inventory(shop: str, token: str, inv_item_id: int, loc_id: int, qty: int, sku: str):
    shopify_request(
        shop, token, "POST", "/inventory_levels/set.json",
        payload={"location_id": loc_id, "inventory_item_id": inv_item_id, "available": qty},
    )
    print(f"  ⚙️  set → {qty}  (SKU {sku}, shop {shop})")

##############################################################################
# Order-hämtning & process
##############################################################################

def fetch_new_orders(shop: str, token: str, since: datetime) -> List[dict]:
    """Hämtar alla ordrar sen 'since'. Inga fields-filter → line_items finns alltid."""
    all_orders: List[dict] = []
    path   = "/orders.json"
    params = {"status": "any", "limit": 250, "created_at_min": since.isoformat()}
    while True:
        resp  = shopify_request(shop, token, "GET", path, params=params)
        batch = resp.json()["orders"]
        all_orders.extend(batch)
        link = resp.headers.get("Link")
        if link and 'rel="next"' in link:
            path = link.split(";")[0].strip("<>").split(f"{API_VERSION}")[1]
            params = {}
        else:
            break
    return all_orders

def process_secondary_orders(
    orders: List[dict],
    sec_shop: str,
    master_shop: str,
    master_token: str,
    master_loc: int,
    master_variants: Dict[str, VariantInfo],
    master_qty: Dict[str, int],
    processed: set[str],
    prefix: str,
) -> List[str]:
    new_ids: List[str] = []

    for o in orders:
        uid = f"{prefix}{o['id']}"
        if uid in processed:
            continue

        lines = o.get("line_items") or []
        print(f"\n🛒 Order {o['id']} ({sec_shop}) – {len(lines)} rader")

        for idx, li in enumerate(lines, start=1):
            sku = (li.get("sku") or "").strip()
            qty = int(li.get("quantity") or 0)
            if not sku or sku not in master_variants:
                print(f"   ⚠️  Rad {idx}: SKU saknas/okänd – hoppar")
                continue

            inv_item_id, _ = master_variants[sku]
            before = master_qty.get(sku, 0)
            after  = before - qty

            print(f"   • Rad {idx}: SKU {sku}  qty {qty}")
            print(f"     Master före: {before}  ➜  efter: {after}")

            adjust_inventory(master_shop, master_token, inv_item_id, master_loc, -qty, sku)
            master_qty[sku] = after

        new_ids.append(uid)

    return new_ids

##############################################################################
# Synk master → sekundär
##############################################################################

def sync_master_to_secondary(
    master_qty: Dict[str, int],
    sec_shop: str,
    sec_token: str,
    sec_loc: int,
    sec_variants: Dict[str, VariantInfo],
):
    sec_qty = fetch_inventory_levels(sec_shop, sec_token, sec_loc, sec_variants)
    updates = 0

    for sku, master_val in master_qty.items():
        if sku not in sec_variants: continue

        current_sec = sec_qty.get(sku, 0)
        diff = master_val - current_sec
        if diff == 0: continue

        inv_item_id, variant_id = sec_variants[sku]

        ensure_trackable(sec_shop, sec_token, variant_id)
        connect_if_needed(sec_shop, sec_token, inv_item_id, sec_loc)

        print(f"\n🔄 Synk SKU {sku}")
        print(f"   Master {master_val}   Sekundär {current_sec}   diff {diff:+}")

        try:
            adjust_inventory(sec_shop, sec_token, inv_item_id, sec_loc, diff, sku)
        except RuntimeError as e:
            print(f"   ⚠️ adjust misslyckades ({e}); fallback set")
            force_set_inventory(sec_shop, sec_token, inv_item_id, sec_loc, master_val, sku)

        updates += 1

    print(f"\n✅ Lager uppdaterat i {sec_shop} för {updates} SKU:er.")

##############################################################################
# MAIN
##############################################################################

def main():
    print("=== Shopify-synk startar ===")

    create_table_if_not_exists()
    processed = load_processed_orders()
    since     = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)

    # Master metadata
    loc1      = get_primary_location_id(SHOP_DOMAIN_1, SHOPIFY_ACCESS_TOKEN_1)
    variants1 = fetch_variants_by_sku(SHOP_DOMAIN_1, SHOPIFY_ACCESS_TOKEN_1)
    master_qty = fetch_inventory_levels(SHOP_DOMAIN_1, SHOPIFY_ACCESS_TOKEN_1, loc1, variants1)

    # Sekundär metadata
    loc2      = get_primary_location_id(SHOP_DOMAIN_2, SHOPIFY_ACCESS_TOKEN_2)
    variants2 = fetch_variants_by_sku(SHOP_DOMAIN_2, SHOPIFY_ACCESS_TOKEN_2)

    # Hämta ordrar
    ord1 = fetch_new_orders(SHOP_DOMAIN_1, SHOPIFY_ACCESS_TOKEN_1, since)
    ord2 = fetch_new_orders(SHOP_DOMAIN_2, SHOPIFY_ACCESS_TOKEN_2, since)

    # Processera butik-2-ordrar
    new_ids_2 = process_secondary_orders(
        orders          = ord2,
        sec_shop        = SHOP_DOMAIN_2,
        master_shop     = SHOP_DOMAIN_1,
        master_token    = SHOPIFY_ACCESS_TOKEN_1,
        master_loc      = loc1,
        master_variants = variants1,
        master_qty      = master_qty,
        processed       = processed,
        prefix          = f"{SHOP_DOMAIN_2}_",
    )

    # Märk butik-1-ordrar som processade (ingen lagerändring behövs)
    new_ids_1 = [str(o["id"]) for o in ord1 if str(o["id"]) not in processed]

    save_processed_orders(new_ids_1 + new_ids_2)
    print(f"\nNya order-ID sparade: {len(new_ids_1) + len(new_ids_2)} st")

    # Synk differenser
    sync_master_to_secondary(
        master_qty,
        SHOP_DOMAIN_2, SHOPIFY_ACCESS_TOKEN_2, loc2, variants2,
    )

    print("\n=== Synk klar ===")

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print("❌ Fatalt fel:", err)
        sys.exit(1)
