#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopify-lager-synk  v3.3   (2025-06-25)
---------------------------------------
• Butik 1 = master
• Full tvåvägssynk med idempotens per order-rad
• Robust mot decimalsträngar (”2.0”) på quantity, inventory och ID-fält
• Skydd mot negativa saldon (kan stängas av med ALLOW_NEGATIVE=true)
• DRY_RUN-läge för säker test
"""

#######################################################################
# Imports & konfiguration
#######################################################################
import os, sys, time, requests, psycopg2, decimal, re
from datetime import datetime, timedelta
from typing import Dict, Tuple, List, Optional

API_VERSION    = "2023-10"                   # senast godkända REST-version
SHOP_1         = os.environ["SHOP_DOMAIN_1"]
TOKEN_1        = os.environ["SHOPIFY_ACCESS_TOKEN_1"]
SHOP_2         = os.environ["SHOP_DOMAIN_2"]
TOKEN_2        = os.environ["SHOPIFY_ACCESS_TOKEN_2"]
DATABASE_URL   = os.environ["DATABASE_URL"]  # postgres://…
LOOKBACK_HOURS = int(os.getenv("SYNC_LOOKBACK_HOURS", "24"))
ALLOW_NEG      = os.getenv("ALLOW_NEGATIVE", "false").lower() == "true"
DRY_RUN        = os.getenv("MODE", "LIVE") == "DRY_RUN"

#######################################################################
# Utils
#######################################################################
def log(msg: str, *, level: str = "INFO"):
    now = datetime.now().strftime("%H:%M:%S")
    colour = {"INFO": "37", "WARN": "33", "ERR": "31"}[level]
    print(f"\x1b[{colour}m[{now}] {msg}\x1b[0m")

_dec_re = re.compile(r"^-?\d+(?:[.,]\d+)?$")
def to_int(val) -> int:
    """Konverterar val till heltal. Hanterar '2', '2.0', '2,0', Decimal, float."""
    if val is None:
        return 0
    if isinstance(val, int):
        return val
    if isinstance(val, (float, decimal.Decimal)):
        return int(round(val))
    if isinstance(val, str) and _dec_re.match(val.strip()):
        return int(decimal.Decimal(val.replace(",", ".")))
    log(f"⚠️  ogiltigt heltal: {val!r}", level="WARN")
    return 0

#######################################################################
# Shopify-wrapper
#######################################################################
def shopify(
    shop: str, token: str, method: str, path: str,
    params: Optional[dict] = None, payload: Optional[dict] = None
) -> requests.Response:
    url = f"https://{shop}/admin/api/{API_VERSION}{path}"
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    while True:
        r = requests.request(method, url, headers=headers,
                             params=params, json=payload, timeout=30)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 2)))
            continue
        if r.status_code >= 300:
            raise RuntimeError(f"{shop} {method} {path} → {r.status_code}: {r.text}")
        return r

#######################################################################
# Databas  (idempotent spårning av order-rader)
#######################################################################
def db(q: str, vals: tuple | list = (), fetch: bool = False):
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(q, vals)
        if fetch:
            return cur.fetchall()

def init_db():
    db("""CREATE TABLE IF NOT EXISTS processed_lines (
            shop     TEXT   NOT NULL,
            line_id  BIGINT NOT NULL,
            PRIMARY KEY (shop, line_id)
          );""")

def is_done(shop: str, lid: int) -> bool:
    return bool(db("SELECT 1 FROM processed_lines WHERE shop=%s AND line_id=%s;",
                   (shop, lid), fetch=True))

def mark_done(shop: str, lids: List[int | str]):
    clean = [to_int(l) for l in lids if to_int(l) != 0]
    if not clean:
        return
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO processed_lines (shop, line_id) VALUES (%s,%s) "
            "ON CONFLICT DO NOTHING;",
            [(shop, lid) for lid in clean]
        )

#######################################################################
# Variant- och lager-rutiner
#######################################################################
Variant = Tuple[int, int]              # (inventory_item_id, variant_id)

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

def inventory(shop: str, tok: str, loc: int,
              vmap: Dict[str, Variant]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    ids = [str(iid) for iid, _ in vmap.values()]
    for i in range(0, len(ids), 50):
        chunk = ",".join(ids[i:i+50])
        levels = shopify(
            shop, tok, "GET", "/inventory_levels.json",
            params={"inventory_item_ids": chunk, "location_ids": loc}
        ).json()["inventory_levels"]
        for lvl in levels:
            avail = to_int(lvl.get("available"))
            sku = next(k for k, (iid, _) in vmap.items()
                       if iid == lvl["inventory_item_id"])
            out[sku] = avail
    return out

def ensure_trackable(shop: str, tok: str, vid: int):
    v = shopify(shop, tok, "GET", f"/variants/{vid}.json").json()["variant"]
    if v["inventory_management"] == "shopify":
        return
    shopify(shop, tok, "PUT", f"/variants/{vid}.json",
            payload={"variant": {"id": vid, "inventory_management": "shopify"}})
    log(f"✓ inventory_management=shopify för variant {vid} ({shop})")

def connect(shop: str, tok: str, iid: int, loc: int):
    try:
        shopify(shop, tok, "POST", "/inventory_levels/connect.json",
                payload={"location_id": loc, "inventory_item_id": iid})
        log(f"✓ connect inventory_item {iid} → loc {loc} ({shop})")
    except RuntimeError as e:
        if "422" not in str(e):
            raise

def adjust(shop: str, tok: str, loc: int, iid: int, delta: int, sku: str):
    if delta == 0:
        return
    if DRY_RUN:
        log(f"[DRY] {shop} adjust {delta:+} (SKU {sku})")
        return
    shopify(shop, tok, "POST", "/inventory_levels/adjust.json",
            payload={"location_id": loc, "inventory_item_id": iid,
                     "available_adjustment": delta})
    log(f"    ↳ justerade {delta:+}  (SKU {sku})")

#######################################################################
# Order-hämtning & rad-processing
#######################################################################
def fetch_orders(shop: str, tok: str, since: datetime) -> List[dict]:
    out: List[dict] = []
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
    orders: List[dict],
    shop_src: str,
    shop_dst: str,
    tok_dst: str,
    loc_dst: int,
    vmap_dst: Dict[str, Variant],
    qty_dst: Dict[str, int],
):
    done: List[int] = []
    for o in orders:
        if o.get("cancelled_at"):
            continue
        for li in o.get("line_items", []):
            lid_raw = li["id"]
            lid = to_int(lid_raw)
            if lid == 0:
                log(f"⚠️  Rad-ID ogiltigt ({lid_raw!r}) – hoppar", level="WARN")
                continue
            if is_done(shop_src, lid):
                continue

            qty = to_int(li.get("quantity"))
            if qty <= 0:
                done.append(lid); continue

            sku = (li.get("sku") or "").strip()
            if not sku or sku not in vmap_dst:
                log(f"⚠️  Hoppar rad {lid}: ogiltig SKU {sku}", level="WARN")
                done.append(lid); continue

            iid, vid = vmap_dst[sku]
            ensure_trackable(shop_dst, tok_dst, vid)
            connect(shop_dst, tok_dst, iid, loc_dst)

            before = qty_dst.get(sku, 0)
            delta  = -qty
            after  = before + delta
            if after < 0 and not ALLOW_NEG:
                delta = -before
                after = 0

            log(f"{shop_src} order {lid}: SKU {sku} qty {qty} → diff {delta:+}")
            adjust(shop_dst, tok_dst, loc_dst, iid, delta, sku)
            qty_dst[sku] = after
            done.append(lid)

    mark_done(shop_src, done)

#######################################################################
# Full-synk master → sekundär
#######################################################################
def full_sync(
    src_qty: Dict[str, int],
    dst_shop: str,
    dst_tok: str,
    dst_loc: int,
    dst_vmap: Dict[str, Variant],
):
    updates = 0
    dst_qty = inventory(dst_shop, dst_tok, dst_loc, dst_vmap)

    for sku, master_val in src_qty.items():
        if sku not in dst_vmap:
            continue
        diff = master_val - dst_qty.get(sku, 0)
        if diff == 0:
            continue
        iid, vid = dst_vmap[sku]
        ensure_trackable(dst_shop, dst_tok, vid)
        connect(dst_shop, dst_tok, iid, dst_loc)
        log(f"Full synk {dst_shop}: SKU {sku} diff {diff:+}")
        adjust(dst_shop, dst_tok, dst_loc, iid, diff, sku)
        updates += 1

    log(f"✓ Full synk klar – {updates} SKU:er uppdaterade i {dst_shop}")

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

    # Order i sekundär butik drar master-lagret
    handle_outgoing(ord2, SHOP_2, SHOP_1, TOKEN_1, loc1, v1, q1)
    # Returer / negativa rader i master kan påverka sekundär
    handle_outgoing(ord1, SHOP_1, SHOP_2, TOKEN_2, loc2, v2, q2)

    # Tabell-synk master → sekundär
    full_sync(q1, SHOP_2, TOKEN_2, loc2, v2)

    log("=== Synk klar ===")

if __name__ == "__main__":
    try:
        mode = "TEST-läge" if DRY_RUN else "LIVE"
        log(f"=== Shopify-synk (v3.3) startar – {mode} ===")
        main()
    except Exception as err:
        log(f"❌ Fatalt fel: {err}", level="ERR")
        sys.exit(1)
