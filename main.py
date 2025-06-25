#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopify‑lager — Butik 2 → Butik 1‑balansering  v1.0  (2025‑06‑25)
-----------------------------------------------------------------
• Butik 1 = master‑lager
• Steg 1: summera utförsäljning i Butik 2 sedan senaste körning
• Steg 2: dra av den mängden från Butik 1‑lagret
• Steg 3: sätt Butik 2‑lagret = aktuellt Butik 1‑lager
• Idempotent per order‑rad (PostgreSQL‑tabell)
• DRY_RUN‑läge för säker test
"""

#######################################################################
# Inställningar (via miljövariabler)
#######################################################################
import os, sys, time, requests, psycopg2, decimal, re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

API_VERSION    = "2023-10"
SHOP_1         = os.environ["SHOP_DOMAIN_1"]          # master‑butik
TOKEN_1        = os.environ["SHOPIFY_ACCESS_TOKEN_1"]
SHOP_2         = os.environ["SHOP_DOMAIN_2"]          # sekundär butik
TOKEN_2        = os.environ["SHOPIFY_ACCESS_TOKEN_2"]
DATABASE_URL   = os.environ["DATABASE_URL"]           # postgres://…
LOOKBACK_HOURS = int(os.getenv("SYNC_LOOKBACK_HOURS", "24"))
ALLOW_NEG      = os.getenv("ALLOW_NEGATIVE", "false").lower() == "true"
DRY_RUN        = os.getenv("MODE", "LIVE") == "DRY_RUN"

#######################################################################
# Bas‑verktyg
#######################################################################
def log(msg: str, level: str = "INFO"):
    colour = {"INFO": "37", "WARN": "33", "ERR": "31"}[level]
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\x1b[{colour}m[{ts}] {msg}\x1b[0m", flush=True)

_dec = re.compile(r"^-?\d+(?:[.,]\d+)?$")
def to_int(v) -> int:
    if v is None: return 0
    if isinstance(v, int): return v
    if isinstance(v, (float, decimal.Decimal)): return int(round(v))
    if isinstance(v, str) and _dec.match(v.strip()):
        return int(decimal.Decimal(v.replace(",", ".")))
    log(f"Ogiltigt tal: {v!r}", "WARN"); return 0

#######################################################################
# Shopify‑hjälpare
#######################################################################
def shopify(shop: str, token: str, method: str, path: str,
            params: Optional[dict] = None,
            payload: Optional[dict] = None) -> requests.Response:
    url = f"https://{shop}/admin/api/{API_VERSION}{path}"
    hdr = {"X-Shopify-Access-Token": token,
           "Content-Type": "application/json", "Accept": "application/json"}
    while True:
        r = requests.request(method, url, headers=hdr,
                             params=params, json=payload, timeout=30)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 2))); continue
        if r.status_code >= 300:
            raise RuntimeError(f"{shop} {method} {path} → {r.status_code}: {r.text}")
        return r

#######################################################################
# DB: spåra hanterade order‑rader
#######################################################################
def db(sql: str, vals: tuple = (), fetch=False):
    with psycopg2.connect(DATABASE_URL) as c, c.cursor() as cur:
        cur.execute(sql, vals)
        return cur.fetchall() if fetch else None

def init_db():
    db("""CREATE TABLE IF NOT EXISTS processed_lines (
            shop TEXT NOT NULL, line_id BIGINT NOT NULL,
            PRIMARY KEY (shop, line_id));
       """)

def mark_done(shop: str, lids: List[int]):
    if not lids: return
    data = [(shop, i) for i in lids]
    with psycopg2.connect(DATABASE_URL) as c, c.cursor() as cur:
        cur.executemany("""INSERT INTO processed_lines (shop,line_id)
                           VALUES (%s,%s) ON CONFLICT DO NOTHING;""", data)

def done(shop: str, lid: int) -> bool:
    return bool(db("SELECT 1 FROM processed_lines WHERE shop=%s AND line_id=%s;",
                   (shop, lid), True))

#######################################################################
# Lager‑API‑rutiner
#######################################################################
Variant = Tuple[int, int]   # (inventory_item_id, variant_id)

def primary_location(shop: str, tok: str) -> int:
    locs = shopify(shop, tok, "GET", "/locations.json").json()["locations"]
    for l in locs:
        if l.get("primary"): return l["id"]
    return locs[0]["id"]

def variants_by_sku(shop: str, tok: str) -> Dict[str, Variant]:
    res, path, params = {}, "/products.json", {"limit": 250, "fields": "variants"}
    while True:
        r = shopify(shop, tok, "GET", path, params=params)
        for p in r.json()["products"]:
            for v in p["variants"]:
                sku = (v["sku"] or "").strip()
                if sku: res[sku] = (v["inventory_item_id"], v["id"])
        link = r.headers.get("Link")
        if link and 'rel="next"' in link:
            path = link.split(";")[0].strip("<>").split(API_VERSION)[1]; params = {}
        else: break
    return res

def inventory(shop: str, tok: str, loc: int,
              vm: Dict[str, Variant]) -> Dict[str, int]:
    out, ids = {}, [str(i) for i, _ in vm.values()]
    for i in range(0, len(ids), 50):
        chunk = ",".join(ids[i:i+50])
        r = shopify(shop, tok, "GET", "/inventory_levels.json",
                    params={"inventory_item_ids": chunk, "location_ids": loc})
        for lvl in r.json()["inventory_levels"]:
            avail = to_int(lvl["available"])
            sku = next(s for s, (iid, _) in vm.items() if iid == lvl["inventory_item_id"])
            out[sku] = avail
    return out

def ensure_trackable(shop: str, tok: str, vid: int):
    v = shopify(shop, tok, "GET", f"/variants/{vid}.json").json()["variant"]
    if v["inventory_management"] != "shopify":
        shopify(shop, tok, "PUT", f"/variants/{vid}.json",
                payload={"variant": {"id": vid, "inventory_management": "shopify"}})

def connect(shop: str, tok: str, iid: int, loc: int):
    try:
        shopify(shop, tok, "POST", "/inventory_levels/connect.json",
                payload={"location_id": loc, "inventory_item_id": iid})
    except RuntimeError as e:
        if "422" not in str(e): raise

def adjust(shop: str, tok: str, loc: int, iid: int, delta: int, sku: str):
    if delta == 0: return
    if DRY_RUN:
        log(f"[DRY] {shop} {sku} {delta:+}")
        return
    shopify(shop, tok, "POST", "/inventory_levels/adjust.json",
            payload={"location_id": loc, "inventory_item_id": iid,
                     "available_adjustment": delta})
    log(f"{shop} {sku} {delta:+}")

#######################################################################
# Order‑hämtning
#######################################################################
def fetch_orders(shop: str, tok: str, since: datetime) -> List[dict]:
    res, path, params = [], "/orders.json", {
        "status": "any", "limit": 250, "created_at_min": since.isoformat()}
    while True:
        r = shopify(shop, tok, "GET", path, params=params); res += r.json()["orders"]
        nxt = r.headers.get("Link")
        if nxt and 'rel="next"' in nxt:
            path = nxt.split(";")[0].strip("<>").split(API_VERSION)[1]; params = {}
        else: break
    return res

#######################################################################
# Steg 1 – summera försäljning i Butik 2
#######################################################################
def sales_in_store2(orders: List[dict],
                    vm2: Dict[str, Variant]) -> Dict[str, int]:
    sold: Dict[str, int] = {}
    processed: List[int] = []
    for o in orders:
        if o.get("cancelled_at"): continue
        for li in o.get("line_items", []):
            lid = to_int(li["id"])
            if lid == 0 or done(SHOP_2, lid): continue
            qty = to_int(li.get("quantity"))
            if qty <= 0: processed.append(lid); continue
            sku = (li.get("sku") or "").strip()
            if sku not in vm2: processed.append(lid); continue
            sold[sku] = sold.get(sku, 0) + qty
            processed.append(lid)
    mark_done(SHOP_2, processed)
    return sold

#######################################################################
# MAIN‑logik
#######################################################################
def main():
    init_db()
    since = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)

    # Hämta grunddata
    loc1, loc2 = primary_location(SHOP_1, TOKEN_1), primary_location(SHOP_2, TOKEN_2)
    vm1, vm2   = variants_by_sku(SHOP_1, TOKEN_1), variants_by_sku(SHOP_2, TOKEN_2)

    # Steg 1: sålda kvantiteter i Butik 2
    sold2 = sales_in_store2(fetch_orders(SHOP_2, TOKEN_2, since), vm2)
    if sold2:
        log(f"– Sålt i Butik 2 sedan senast: {sold2}")

    # Steg 2: minska lagret i Butik 1
    inv1 = inventory(SHOP_1, TOKEN_1, loc1, vm1)
    for sku, qty in sold2.items():
        if sku not in vm1: continue
        iid, vid = vm1[sku]
        ensure_trackable(SHOP_1, TOKEN_1, vid)
        connect(SHOP_1, TOKEN_1, iid, loc1)
        delta = -qty
        if inv1.get(sku, 0) + delta < 0 and not ALLOW_NEG:
            delta = -inv1.get(sku, 0)
        adjust(SHOP_1, TOKEN_1, loc1, iid, delta, sku)
        inv1[sku] = inv1.get(sku, 0) + delta

    # Steg 3: sätt Butik 2 = Butik 1
    inv2 = inventory(SHOP_2, TOKEN_2, loc2, vm2)
    for sku, qty1 in inv1.items():
        if sku not in vm2: continue
        diff = qty1 - inv2.get(sku, 0)
        if diff == 0: continue
        iid2, vid2 = vm2[sku]
        ensure_trackable(SHOP_2, TOKEN_2, vid2)
        connect(SHOP_2, TOKEN_2, iid2, loc2)
        adjust(SHOP_2, TOKEN_2, loc2, iid2, diff, sku)

    log("✓ Synk färdig – Butik 2 matchar nu Butik 1")

#######################################################################
if __name__ == "__main__":
    try:
        log(f"=== Lager‑synk startar  (mode={'DRY_RUN' if DRY_RUN else 'LIVE'}) ===")
        main()
    except Exception as e:
        log(f"❌ Fel: {e}", "ERR")
        sys.exit(1)
