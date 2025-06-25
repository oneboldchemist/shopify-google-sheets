#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopify-lager — Butik 2 → Butik 1-balansering  **v1.2**  (2025-06-25)
---------------------------------------------------------------------
• Butik 1 = master-lager  
• Steg 1: summera utförsäljning i Butik 2 sedan senaste körning  
• Steg 2: dra av den mängden från Butik 1-lagret  
• Steg 3: sätt Butik 2-lagret = aktuellt Butik 1-lager  
• Idempotent per order-rad (tabell *processed_lines*)  
• FULL VERBOSITET – visar även när rader hoppas över  
• DRY_RUN-läge för säker test
"""

#######################################################################
# Inställningar (via miljövariabler)
#######################################################################
import os, sys, time, requests, psycopg2, decimal, json, re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

API_VERSION    = "2023-10"
SHOP_1         = os.environ["SHOP_DOMAIN_1"]          # master-butik
TOKEN_1        = os.environ["SHOPIFY_ACCESS_TOKEN_1"]
SHOP_2         = os.environ["SHOP_DOMAIN_2"]          # sekundär butik
TOKEN_2        = os.environ["SHOPIFY_ACCESS_TOKEN_2"]
DATABASE_URL   = os.environ["DATABASE_URL"]           # postgres://…
LOOKBACK_HOURS = int(os.getenv("SYNC_LOOKBACK_HOURS", "24"))
ALLOW_NEG      = os.getenv("ALLOW_NEGATIVE", "false").lower() == "true"
DRY_RUN        = os.getenv("MODE", "LIVE") == "DRY_RUN"

#######################################################################
# Bas-verktyg
#######################################################################
def log(msg: str, level: str = "INFO"):
    colours = {"INFO": "37", "WARN": "33", "ERR": "31", "DEBUG": "36"}
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\x1b[{colours.get(level,'37')}m[{ts}] {msg}\x1b[0m", flush=True)

_dec = re.compile(r"^-?\d+(?:[.,]\d+)?$")
def to_int(v) -> int:
    if v is None: return 0
    if isinstance(v, int): return v
    if isinstance(v, (float, decimal.Decimal)): return int(round(v))
    if isinstance(v, str) and _dec.match(v.strip()):
        return int(decimal.Decimal(v.replace(",", ".")))
    log(f"Ogiltigt tal: {v!r}", "WARN"); return 0

def pretty(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)

#######################################################################
# Shopify-hjälpare
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
            wait = int(r.headers.get("Retry-After", 2))
            log(f"{shop}: rate-limited, väntar {wait}s", "DEBUG"); time.sleep(wait); continue
        if r.status_code >= 300:
            raise RuntimeError(f"{shop} {method} {path} → {r.status_code}: {r.text}")
        return r

#######################################################################
# DB – idempotent spårning av order-rader
#######################################################################
def db(sql: str, vals: tuple = (), fetch=False):
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(sql, vals)
        return cur.fetchall() if fetch else None

def init_db():
    db("""
        CREATE TABLE IF NOT EXISTS processed_lines (
            shop TEXT NOT NULL,
            line_id BIGINT NOT NULL,
            PRIMARY KEY (shop, line_id)
        );
    """)

def mark_done(shop: str, lids: List[int]):
    if not lids: return
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO processed_lines (shop, line_id)
            VALUES (%s, %s) ON CONFLICT DO NOTHING;
        """, [(shop, lid) for lid in lids])
    log(f"{shop}: markerade {len(lids)} rader som behandlade", "DEBUG")

def done(shop: str, lid: int) -> bool:
    return bool(db("SELECT 1 FROM processed_lines WHERE shop=%s AND line_id=%s;",
                   (shop, lid), True))

#######################################################################
# Lager-API-rutiner
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
        nxt = r.headers.get("Link")
        if nxt and 'rel="next"' in nxt:
            path = nxt.split(";")[0].strip("<>").split(API_VERSION)[1]; params = {}
        else: break
    log(f"{shop}: hittade {len(res)} SKU-varianter", "DEBUG")
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
    log(f"{shop}: lager på loc {loc}: {pretty(out)}", "DEBUG")
    return out

def ensure_trackable(shop: str, tok: str, vid: int):
    v = shopify(shop, tok, "GET", f"/variants/{vid}.json").json()["variant"]
    if v["inventory_management"] != "shopify":
        shopify(shop, tok, "PUT", f"/variants/{vid}.json",
                payload={"variant": {"id": vid, "inventory_management": "shopify"}})
        log(f"{shop}: inventory_management → shopify för variant {vid}", "DEBUG")

def connect(shop: str, tok: str, iid: int, loc: int):
    try:
        shopify(shop, tok, "POST", "/inventory_levels/connect.json",
                payload={"location_id": loc, "inventory_item_id": iid})
        log(f"{shop}: connect inventory_item {iid} → loc {loc}", "DEBUG")
    except RuntimeError as e:
        if "422" not in str(e): raise

def adjust(shop: str, tok: str, loc: int,
           iid: int, delta: int, sku: str,
           before: int, label: str):
    if delta == 0: return
    if DRY_RUN:
        log(f"[DRY] {label}: SKU {sku} {before} → {before+delta}  (Δ {delta:+})")
        return
    shopify(shop, tok, "POST", "/inventory_levels/adjust.json",
            payload={"location_id": loc, "inventory_item_id": iid,
                     "available_adjustment": delta})
    log(f"{label}: SKU {sku} {before} → {before+delta}  (Δ {delta:+})")

#######################################################################
# Order-hämtning & utskrift
#######################################################################
def fetch_orders(shop: str, tok: str, since: datetime) -> List[dict]:
    res, path, params = [], "/orders.json", {
        "status": "any", "limit": 250, "created_at_min": since.isoformat()}
    while True:
        r = shopify(shop, tok, "GET", path, params=params)
        res += r.json()["orders"]
        nxt = r.headers.get("Link")
        if nxt and 'rel="next"' in nxt:
            path = nxt.split(";")[0].strip("<>").split(API_VERSION)[1]; params = {}
        else: break
    log(f"{shop}: hämtade {len(res)} ordrar", "DEBUG")
    return res

def print_orders(orders: List[dict], label: str):
    for o in orders:
        log(f"{label}: Order {o['id']}  #{o['name']}  skapad {o['created_at']}")
        for li in o.get("line_items", []):
            log(f"   └─ Rad {li['id']}  SKU {li['sku']}  QTY {li['quantity']}")

#######################################################################
# Steg 1 – summera försäljning i Butik 2
#######################################################################
def sales_in_store2(orders: List[dict],
                    vm2: Dict[str, Variant]) -> Dict[str, int]:
    sold: Dict[str, int] = {}
    processed: List[int] = []

    # Filtrera fram ordrar med MINST en ny rad
    orders_with_new = [
        o for o in orders
        if any(not done(SHOP_2, to_int(li["id"])) for li in o.get("line_items", []))
    ]
    print_orders(orders_with_new, "Butik 2 – NYA rader")

    for o in orders_with_new:
        if o.get("cancelled_at"): continue
        for li in o.get("line_items", []):
            lid = to_int(li["id"])
            if lid == 0:
                log(f"⤼ Skippar rad utan giltigt ID ({li['id']})", "WARN")
                continue
            if done(SHOP_2, lid):
                log(f"⤼ Skippar rad {lid} (redan behandlad)", "DEBUG")
                continue

            qty = to_int(li.get("quantity"))
            if qty <= 0:
                processed.append(lid); continue
            sku = (li.get("sku") or "").strip()
            if sku not in vm2:
                log(f"⤼ Skippar rad {lid}: okänd SKU {sku}", "WARN")
                processed.append(lid); continue

            sold[sku] = sold.get(sku, 0) + qty
            processed.append(lid)
            log(f"Butik 2 sålt: rad {lid}  SKU {sku}  QTY {qty}  (tot {sold[sku]})")

    mark_done(SHOP_2, processed)
    return sold

#######################################################################
# MAIN-flöde
#######################################################################
def main():
    init_db()
    since = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)
    log(f"Ser tillbaka {LOOKBACK_HOURS}h ⇒ {since.isoformat()}", "DEBUG")

    loc1, loc2 = primary_location(SHOP_1, TOKEN_1), primary_location(SHOP_2, TOKEN_2)
    vm1,  vm2  = variants_by_sku(SHOP_1, TOKEN_1), variants_by_sku(SHOP_2, TOKEN_2)

    # --- Steg 1 -------------------------------------------------------
    log("=== STEG 1: Hämta nya orderrader från Butik 2 ===")
    orders2 = fetch_orders(SHOP_2, TOKEN_2, since)
    sold2   = sales_in_store2(orders2, vm2)
    log(f"Summerad försäljning Butik 2: {pretty(sold2)}")

    # --- Steg 2 -------------------------------------------------------
    log("=== STEG 2: Minska Butik 1-lager med sålda kvantiteter ===")
    inv1 = inventory(SHOP_1, TOKEN_1, loc1, vm1)
    for sku, qty in sold2.items():
        if sku not in vm1: continue
        iid, vid = vm1[sku]
        ensure_trackable(SHOP_1, TOKEN_1, vid)
        connect(SHOP_1, TOKEN_1, iid, loc1)

        before = inv1.get(sku, 0)
        delta  = -qty
        if before + delta < 0 and not ALLOW_NEG:
            delta = -before
        adjust(SHOP_1, TOKEN_1, loc1, iid, delta, sku, before, "Butik 1")
        inv1[sku] = before + delta

    log(f"Ny lagerstatus Butik 1: {pretty(inv1)}")

    # --- Steg 3 -------------------------------------------------------
    log("=== STEG 3: Synka Butik 2 → samma saldo som Butik 1 ===")
    inv2 = inventory(SHOP_2, TOKEN_2, loc2, vm2)
    for sku, qty1 in inv1.items():
        if sku not in vm2: continue
        diff = qty1 - inv2.get(sku, 0)
        if diff == 0: continue
        iid2, vid2 = vm2[sku]
        ensure_trackable(SHOP_2, TOKEN_2, vid2)
        connect(SHOP_2, TOKEN_2, iid2, loc2)
        adjust(SHOP_2, TOKEN_2, loc2, iid2, diff, sku, inv2.get(sku, 0), "Butik 2")
        inv2[sku] = qty1

    log(f"Ny lagerstatus Butik 2: {pretty(inv2)}")
    log("✓ Synk klar – Butik 2 matchar nu Butik 1\n")

#######################################################################
if __name__ == "__main__":
    try:
        mode = "DRY_RUN" if DRY_RUN else "LIVE"
        log(f"=== Shopify-lager-synk startar  [mode={mode}] ===")
        main()
    except Exception as e:
        log(f"❌ Fel: {e}", "ERR")
        sys.exit(1)
