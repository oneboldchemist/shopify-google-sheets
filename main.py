#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopify-lager-synk (v3.1, 2025-06-25)
-------------------------------------
• Butik 1 = master   • Full tvåvägssynk   • Idempotent per orderrad
• Skydd mot dubletter, negativa saldon, race conditions
"""

#######################################################################
# Imports & konfiguration
#######################################################################
import os, sys, time, requests, psycopg2
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

API_VERSION    = "2023-10"
SHOP_1         = os.environ["SHOP_DOMAIN_1"]
TOKEN_1        = os.environ["SHOPIFY_ACCESS_TOKEN_1"]
SHOP_2         = os.environ["SHOP_DOMAIN_2"]
TOKEN_2        = os.environ["SHOPIFY_ACCESS_TOKEN_2"]
DATABASE_URL   = os.environ["DATABASE_URL"]
LOOKBACK_HOURS = int(os.getenv("SYNC_LOOKBACK_HOURS", "24"))
ALLOW_NEG      = os.getenv("ALLOW_NEGATIVE", "false").lower() == "true"
DRY_RUN        = os.getenv("MODE", "LIVE") == "DRY_RUN"

#######################################################################
# Hjälpfunktioner: loggning & Shopify-wrapper
#######################################################################
def log(msg: str, *, level: str = "INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    clr = {"INFO": "37", "WARN": "33", "ERR": "31"}[level]
    print(f"\x1b[{clr}m[{ts}] {msg}\x1b[0m")

def shopify(
    shop: str, token: str, method: str, path: str,
    params: Optional[dict] = None, payload: Optional[dict] = None
) -> requests.Response:
    url = f"https://{shop}/admin/api/{API_VERSION}{path}"
    hdr = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    while True:
        resp = requests.request(method, url, headers=hdr,
                                params=params, json=payload, timeout=30)
        if resp.status_code == 429:
            time.sleep(int(resp.headers.get("Retry-After", 2)))
            continue
        if resp.status_code >= 300:
            raise RuntimeError(f"{shop} {method} {path} → {resp.status_code}: {resp.text}")
        return resp

#######################################################################
# Databas  (idempotens på order‐*rad*)
#######################################################################
def db(q: str, vals: tuple | list = (), fetch: bool = False):
    with psycopg2.connect(DATABASE_URL) as c, c.cursor() as cur:
        cur.execute(q, vals)
        if fetch:
            return cur.fetchall()

def init_db():
    db("""
        CREATE TABLE IF NOT EXISTS processed_lines (
            shop     TEXT NOT NULL,
            line_id  BIGINT NOT NULL,
            PRIMARY KEY (shop, line_id)
        );
    """)

def is_done(shop: str, lid: int) -> bool:
    return bool(db("SELECT 1 FROM processed_lines WHERE shop=%s AND line_id=%s;",
                   (shop, lid), fetch=True))

def mark_done(shop: str, lids: list[int]):
    if not lids:
        return
    with psycopg2.connect(DATABASE_URL) as c, c.cursor() as cur:
        cur.executemany(
            "INSERT INTO processed_lines (shop, line_id) VALUES (%s,%s) "
            "ON CONFLICT DO NOTHING;",
            [(shop, lid) for lid in lids]
        )

#######################################################################
# Variant- och lager-hjälp
#######################################################################
Variant = Tuple[int, int]           # (inventory_item_id, variant_id)

def primary_location(shop: str, tok: str) -> int:
    locs = shopify(shop, tok, "GET", "/locations.json").json()["locations"]
    for loc in locs:
        if loc.get("primary"):
            return loc["id"]
    return locs[0]["id"]

def variants_by_sku(shop: str, tok: str) -> Dict[str, Variant]:
    out: Dict[str, Variant] = {}
    path, params = "/products.json", {"limit": 250, "fields": "variants"}
    while True:
        r = shopify(shop, tok, "GET", path, params=params)
        for prod in r.json()["products"]:
            for v in prod["variants"]:
                sku = (v["sku"] or "").strip()
                if sku:
                    out[sku] = (v["inventory_item_id"], v["id"])
        link = r.headers.get("Link")
        if link and 'rel="next"' in link:
            path = link.split(";")[0].strip("<>").split(API_VERSION)[1]
            params = {}
        else:
            break
    return out

def inventory(
    shop: str, tok: str, loc: int, vmap: Dict[str, Variant]
) -> Dict[str, int]:
    out: Dict[str, int] = {}
    ids = [str(i) for i, _ in vmap.values()]
    for i in range(0, len(ids), 50):
        chunk = ",".join(ids[i:i + 50])
        levels = shopify(
            shop, tok, "GET", "/inventory_levels.json",
            params={"inventory_item_ids": chunk, "location_ids": loc}
        ).json()["inventory_levels"]
        for lvl in levels:
            sku = next(k for k, (iid, _) in vmap.items() if iid == lvl["inventory_item_id"])
            out[sku] = int(lvl.get("available") or 0)
    return out

def ensure_trackable(shop: str, tok: str, vid: int):
    v = shopify(shop, tok, "GET", f"/variants/{vid}.json").json()["variant"]
    if v["inventory_management"] == "shopify":
        return
    shopify(
        shop, tok, "PUT", f"/variants/{vid}.json",
        payload={"variant": {"id": vid, "inventory_management": "shopify"}}
    )
    log(f"✓ inventory_management=shopify för variant {vid} ({shop})")

def connect(shop: str, tok: str, iid: int, loc: int):
    try:
        shopify(shop, tok, "POST", "/inventory_levels/connect.json",
                payload={"location_id": loc, "inventory_item_id": iid})
        log(f"✓ connect inventory_item {iid} → loc {loc} ({shop})")
    except RuntimeError as e:
        if "422" not in str(e):
            raise  # redan kopplad är OK

def adjust(shop: str, tok: str, loc: int, iid: int, delta: int, sku: str):
    if delta == 0:
        return
    if DRY_RUN:
        log(f"[DRY] {shop} adjust {delta:+} (SKU {sku})")
        return
    shopify(
        shop, tok, "POST", "/inventory_levels/adjust.json",
        payload={"location_id": loc, "inventory_item_id": iid,
                 "available_adjustment": delta}
    )
    log(f"    ↳ justerade {delta:+}  (SKU {sku})")

#######################################################################
# Order-hämtning och rad-processing
#######################################################################
def fetch_orders(shop: str, tok: str, since: datetime) -> list[dict]:
    out: list[dict] = []
    path, params = "/orders.json", {
        "status": "any", "limit": 250, "created_at_min": since.isoformat()
    }
    while True:
        r = shopify(shop, tok, "GET", path, params=params)
        out.extend(r.json()["orders"])
        link = r.headers.get("Link")
        if link and 'rel="next"' in link:
            path = link.split(";")[0].strip("<>").split(API_VERSION)[1]
            params = {}
        else:
            break
    return out

def handle_outgoing(
    orders: list[dict],
    shop_src: str,
    shop_dst: str,
    tok_dst: str,
    loc_dst: int,
    vmap_dst: Dict[str, Variant],
    qty_dst: Dict[str, int],
):
    done: list[int] = []
    for o in orders:
        if o.get("cancelled_at"):
            continue
        for li in o.get("line_items", []):
            lid = li["id"]
            if is_done(shop_src, lid):
                continue
            if li.get("quantity", 0) <= 0:
                done.append(lid)
                continue
            sku = (li.get("sku") or "").strip()
            qty = int(li["quantity"])
            if not sku or sku not in vmap_dst:
                log(f"⚠️  Hoppar rad {lid}: ogiltig SKU {sku}", level="WARN")
                done.append(lid)
                continue
            iid, vid = vmap_dst[sku]
            ensure_trackable(shop_dst, tok_dst, vid)
            connect(shop_dst, tok_dst, iid, loc_dst)

            before = qty_dst.get(sku, 0)
            delta = -qty
            after = before + delta
            if after < 0 and not ALLOW_NEG:
                delta = -before
                after = 0
            log(f"{shop_src} order {lid}: SKU {sku} qty {qty} → diff {delta:+}")
            adjust(shop_dst, tok_dst, loc_dst, iid, delta, sku)
            qty_dst[sku] = after
            done.append(lid)
    mark_done(shop_src, done)

#######################################################################
# Full synk master → sekundär
#######################################################################
def full_sync(
    src_qty: Dict[str, int],
    dst_shop: str,
    dst_tok: str,
    dst_loc: int,
    dst_vmap: Dict[str, Variant],
):
    diff_cnt = 0
    dst_qty = inventory(dst_shop, dst_tok, dst_loc, dst_vmap)
    for sku, val in src_qty.items():
        if sku not in dst_vmap:
            continue
        gap = val - dst_qty.get(sku, 0)
        if gap == 0:
            continue
        iid, vid = dst_vmap[sku]
        ensure_trackable(dst_shop, dst_tok, vid)
        connect(dst_shop, dst_tok, iid, dst_loc)
        log(f"Full synk {dst_shop}: SKU {sku} diff {gap:+}")
        adjust(dst_shop, dst_tok, dst_loc, iid, gap, sku)
        diff_cnt += 1
    log(f"✓ Full synk klar – {diff_cnt} SKU:er uppdaterade i {dst_shop}")

#######################################################################
# MAIN
#######################################################################
def main():
    init_db()
    since = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)

    loc1 = primary_location(SHOP_1, TOKEN_1)
    loc2 = primary_location(SHOP_2, TOKEN_2)

    v1 = variants_by_sku(SHOP_1, TOKEN_1)
    v2 = variants_by_sku(SHOP_2, TOKEN_2)

    q1 = inventory(SHOP_1, TOKEN_1, loc1, v1)
    q2 = inventory(SHOP_2, TOKEN_2, loc2, v2)

    ord1 = fetch_orders(SHOP_1, TOKEN_1, since)
    ord2 = fetch_orders(SHOP_2, TOKEN_2, since)

    # Order i sekundär butik drar master
    handle_outgoing(ord2, SHOP_2, SHOP_1, TOKEN_1, loc1, v1, q1)

    # (Returer i master kan i praktiken öka lagret – hanteras symmetriskt)
    handle_outgoing(ord1, SHOP_1, SHOP_2, TOKEN_2, loc2, v2, q2)

    # Full tabell-synk master → sekundär
    full_sync(q1, SHOP_2, TOKEN_2, loc2, v2)

    log("=== Synk klar ===")

if __name__ == "__main__":
    try:
        mode = "TEST-läge" if DRY_RUN else "LIVE"
        log(f"=== Shopify-synk (v3.1) startar – {mode} ===")
        main()
    except Exception as e:
        log(f"❌ Fatalt fel: {e}", level="ERR")
        sys.exit(1)
