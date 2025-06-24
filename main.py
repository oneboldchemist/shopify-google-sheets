#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Synkroniserar lager mellan två separata Shopify‑butiker.

• Butik 1 (huvudbutiken) är “source of truth”.
• All manuell lagerjustering i butik 1 kopieras direkt till butik 2.
• Försäljning i båda butikerna drar från ett gemensamt lager:
    – När butik 1 säljer → Shopify drar automatiskt från dess lager.
      Skriptet synkar därefter samma nya saldo till butik 2.
    – När butik 2 säljer → Shopify drar automatiskt från dess lager.
      Skriptet justerar därefter butik 1 (samma kvantitet) och
      synkar därefter tillbaka saldot från butik 1 → butik 2.
• Alla ordrar innehåller “riktiga” parfymprodukter, inga bundlar.
• Matchning sker på SKU (måste vara identisk i båda butikerna).
"""

##############################################################################
#                                Importer                                    #
##############################################################################

import os
import sys
import time
import json
import math
import psycopg2
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

##############################################################################
#                          Miljö‑ & konfig‑variabler                          #
##############################################################################

API_VERSION = "2023-07"

# === Butik 1 (huvudbutik) ===
SHOP_DOMAIN_1          = os.getenv("SHOP_DOMAIN_1")          or "first-shop.myshopify.com"
SHOPIFY_ACCESS_TOKEN_1 = os.getenv("SHOPIFY_ACCESS_TOKEN_1") or "access-token-shop-1"

# === Butik 2 (sekundär) ===
SHOP_DOMAIN_2          = os.getenv("SHOP_DOMAIN_2")          or "second-shop.myshopify.com"
SHOPIFY_ACCESS_TOKEN_2 = os.getenv("SHOPIFY_ACCESS_TOKEN_2") or "access-token-shop-2"

# Postgres: lagrar endast redan processade order‑ID
DATABASE_URL = os.getenv("DATABASE_URL") or "postgres://..."

# Hur långt tillbaka (timmar) nya ordrar ska hämtas varje körning
LOOKBACK_HOURS = int(os.getenv("SYNC_LOOKBACK_HOURS", "24"))

##############################################################################
#                           PostgreSQL‑hjälpfunktioner                       #
##############################################################################

def create_table_if_not_exists() -> None:
    """Skapar tabellen processed_orders om den saknas."""
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_orders (
                order_id    TEXT PRIMARY KEY,
                processed_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """
        )

def load_processed_orders() -> set:
    """Returnerar en mängd med redan hanterade order‑ID:n."""
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute("SELECT order_id FROM processed_orders;")
        return {row[0] for row in cur.fetchall()}

def save_processed_orders(order_ids: List[str]) -> None:
    """Sparar nya order‑ID:n, ignorerar dubbletter."""
    if not order_ids:
        return
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO processed_orders (order_id)
            VALUES (%s)
            ON CONFLICT (order_id) DO NOTHING;
            """,
            [(oid,) for oid in order_ids],
        )

##############################################################################
#                            Shopify API‑hjälp                               #
##############################################################################

def shopify_request(
    shop_domain: str,
    access_token: str,
    method: str,
    path: str,
    params: dict | None = None,
    payload: dict | None = None,
) -> requests.Response:
    """Wrapper med enkel retry & rate‑limit‑sleep."""
    base = f"https://{shop_domain}/admin/api/{API_VERSION}"
    url  = f"{base}{path}"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type":          "application/json",
        "Accept":                "application/json",
    }
    while True:
        resp = requests.request(method, url, headers=headers, params=params, json=payload)
        if resp.status_code == 429:           # rate limit
            retry_after = int(resp.headers.get("Retry-After", 2))
            time.sleep(retry_after)
            continue
        if resp.status_code >= 400:
            raise RuntimeError(f"{shop_domain} {method} {path} → {resp.status_code}: {resp.text}")
        return resp


##############################################################################
#                      Hämta plats‑ID (location_id)                          #
##############################################################################

def get_primary_location_id(shop_domain: str, token: str) -> int:
    """Returnerar primary location_id för given butik."""
    resp = shopify_request(shop_domain, token, "GET", "/locations.json")
    locations = resp.json()["locations"]
    # Försök hitta primary; annars första aktiva.
    for loc in locations:
        if loc.get("primary"):
            return loc["id"]
    return locations[0]["id"]


##############################################################################
#             Hämta varianter & lager (inventory_item_id / qty)              #
##############################################################################

VariantInfo = Tuple[int, int]  # (inventory_item_id, variant_id)

def fetch_variants_by_sku(
    shop_domain: str,
    token: str,
) -> Dict[str, VariantInfo]:
    """Returnerar dict SKU -> (inventory_item_id, variant_id)."""
    variants = {}
    endpoint = "/products.json"
    params   = {"limit": 250, "fields": "id,variants"}
    while True:
        resp = shopify_request(shop_domain, token, "GET", endpoint, params=params)
        data = resp.json()["products"]
        for product in data:
            for v in product["variants"]:
                sku = (v["sku"] or "").strip()
                if not sku:
                    continue
                variants[sku] = (v["inventory_item_id"], v["id"])
        # pagination
        link = resp.headers.get("Link")
        if link and 'rel="next"' in link:
            next_url = link.split(";")[0].strip("<>")
            # Använd absolut‑URL som path i nästa anrop
            endpoint = next_url.replace(f"https://{shop_domain}/admin/api/{API_VERSION}", "")
            params   = {}
        else:
            break
    return variants


def fetch_inventory_levels(
    shop_domain: str,
    token: str,
    location_id: int,
    variants_map: Dict[str, VariantInfo],
) -> Dict[str, int]:
    """Returnerar dict SKU -> kvantitet på angiven location."""
    sku_to_qty: Dict[str, int] = {}
    inv_item_ids = [str(info[0]) for info in variants_map.values()]
    # Shopify tillåter max 50 inventory_item_ids per anrop.
    chunk_size = 50
    for i in range(0, len(inv_item_ids), chunk_size):
        chunk_ids = ",".join(inv_item_ids[i : i + chunk_size])
        path = "/inventory_levels.json"
        params = {"inventory_item_ids": chunk_ids, "location_ids": location_id}
        resp = shopify_request(shop_domain, token, "GET", path, params=params)
        for level in resp.json()["inventory_levels"]:
            inv_item_id = level["inventory_item_id"]
            qty         = level["available"]
            # hitta motsvarande SKU
            for sku, info in variants_map.items():
                if info[0] == inv_item_id:
                    sku_to_qty[sku] = qty
                    break
    return sku_to_qty


##############################################################################
#                         Lager‑justering & synkning                         #
##############################################################################

def adjust_inventory(
    shop_domain: str,
    token: str,
    inventory_item_id: int,
    location_id: int,
    adjustment: int,
) -> None:
    """Justera (±) lagersaldo."""
    payload = {
        "location_id":          location_id,
        "inventory_item_id":    inventory_item_id,
        "available_adjustment": adjustment,
    }
    shopify_request(shop_domain, token, "POST", "/inventory_levels/adjust.json", payload=payload)


def set_inventory(
    shop_domain: str,
    token: str,
    inventory_item_id: int,
    location_id: int,
    new_quantity: int,
) -> None:
    """Sätt exakt lagersaldo (ersätter befintligt)."""
    payload = {
        "location_id":       location_id,
        "inventory_item_id": inventory_item_id,
        "available":         new_quantity,
    }
    shopify_request(shop_domain, token, "POST", "/inventory_levels/set.json", payload=payload)


##############################################################################
#                           Hämta nya ordrar                                 #
##############################################################################

def fetch_new_orders(
    shop_domain: str,
    token: str,
    since: datetime,
) -> List[dict]:
    """Returnerar alla ordrar skapade sedan 'since'."""
    orders: List[dict] = []
    path   = "/orders.json"
    params = {
        "status":         "any",
        "limit":          250,
        "created_at_min": since.isoformat(),
        "fields": (
            "id,created_at,line_items(id,sku,quantity,title)"
        ),
    }
    while True:
        resp = shopify_request(shop_domain, token, "GET", path, params=params)
        batch = resp.json()["orders"]
        orders.extend(batch)
        link = resp.headers.get("Link")
        if link and 'rel="next"' in link:
            next_url = link.split(";")[0].strip("<>")
            path   = next_url.replace(f"https://{shop_domain}/admin/api/{API_VERSION}", "")
            params = {}
        else:
            break
    return orders


##############################################################################
#                       Processa och synka försäljning                       #
##############################################################################

def process_orders_and_adjust_master(
    source_shop_domain: str,
    source_token: str,
    source_location_id: int,
    source_variants: Dict[str, VariantInfo],
    dest_shop_domain: str,
    dest_token: str,
    orders: List[dict],
    already_processed: set,
    prefix: str,
) -> List[str]:
    """
    Hanterar ordrar från dest_shop_domain (butik 2) och drar motsvarande
    kvantitet från master‑lagret (butik 1). Returnerar nya order‑ID:n.
    """
    new_ids: List[str] = []

    for order in orders:
        raw_id        = str(order["id"])
        unique_order  = f"{prefix}{raw_id}"
        if unique_order in already_processed:
            continue

        for li in order["line_items"]:
            sku = (li.get("sku") or "").strip()
            qty = int(li["quantity"])
            if not sku or sku not in source_variants:
                continue  # okända SKU:er ignoreras
            inv_item_id, _ = source_variants[sku]
            adjust_inventory(
                source_shop_domain,
                source_token,
                inv_item_id,
                source_location_id,
                -qty,                         # minus eftersom det är försäljning
            )
        new_ids.append(unique_order)

    return new_ids


##############################################################################
#                                 MAIN                                       #
##############################################################################

def main() -> None:
    print("=== Shopify Lager‑synk startar ===")
    create_table_if_not_exists()
    processed = load_processed_orders()

    # Tidsfönster för orderhämtning
    since = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)

    # ── 1. Förbered data för båda butiker ────────────────────────────────── #
    loc_id_1 = get_primary_location_id(SHOP_DOMAIN_1, SHOPIFY_ACCESS_TOKEN_1)
    loc_id_2 = get_primary_location_id(SHOP_DOMAIN_2, SHOPIFY_ACCESS_TOKEN_2)

    variants_1 = fetch_variants_by_sku(SHOP_DOMAIN_1, SHOPIFY_ACCESS_TOKEN_1)
    variants_2 = fetch_variants_by_sku(SHOP_DOMAIN_2, SHOPIFY_ACCESS_TOKEN_2)

    # ── 2. Hämta nya ordrar ──────────────────────────────────────────────── #
    orders_1 = fetch_new_orders(SHOP_DOMAIN_1, SHOPIFY_ACCESS_TOKEN_1, since)
    orders_2 = fetch_new_orders(SHOP_DOMAIN_2, SHOPIFY_ACCESS_TOKEN_2, since)

    # ── 3. Markera ordrar i butik 1 (behöver ingen lager‑justering) ─────── #
    new_ids_1 = []
    for o in orders_1:
        oid = str(o["id"])
        if oid not in processed:
            new_ids_1.append(oid)

    # ── 4. Justera master‑lager för försäljning i butik 2 ────────────────── #
    new_ids_2 = process_orders_and_adjust_master(
        source_shop_domain = SHOP_DOMAIN_1,
        source_token       = SHOPIFY_ACCESS_TOKEN_1,
        source_location_id = loc_id_1,
        source_variants    = variants_1,
        dest_shop_domain   = SHOP_DOMAIN_2,
        dest_token         = SHOPIFY_ACCESS_TOKEN_2,
        orders             = orders_2,
        already_processed  = processed,
        prefix             = f"{SHOP_DOMAIN_2}_",
    )

    # ── 5. Spara nya order‑ID:n i databasen ─────────────────────────────── #
    save_processed_orders(new_ids_1 + new_ids_2)
    print(f"Nya order‑ID sparade: {len(new_ids_1) + len(new_ids_2)} st")

    # ── 6. Synka aktuellt lager från butik 1 → butik 2 ───────────────────── #
    qty_1 = fetch_inventory_levels(
        SHOP_DOMAIN_1, SHOPIFY_ACCESS_TOKEN_1, loc_id_1, variants_1
    )
    qty_2 = fetch_inventory_levels(
        SHOP_DOMAIN_2, SHOPIFY_ACCESS_TOKEN_2, loc_id_2, variants_2
    )

    updates = 0
    for sku, master_qty in qty_1.items():
        if sku not in variants_2:
            continue
        if qty_2.get(sku) == master_qty:
            continue  # redan samma saldo
        inv_item_id_2, _ = variants_2[sku]
        set_inventory(
            SHOP_DOMAIN_2,
            SHOPIFY_ACCESS_TOKEN_2,
            inv_item_id_2,
            loc_id_2,
            master_qty,
        )
        updates += 1

    print(f"Lagersaldo uppdaterat i butik 2 för {updates} SKU:er.")
    print("=== Synk färdig ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("Fel i synken:", exc)
        sys.exit(1)
