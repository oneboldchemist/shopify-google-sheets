#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopify-lager — Butik 2 → Butik 1-balansering  v1.3  (2025-06-25)
------------------------------------------------------------------
• Butik 1 = master-lager  
• Steg 1: summera utförsäljning i Butik 2 (endast NYA orderrader)  
• Steg 2: dra av kvantiteten från Butik 1-lagret  
• Steg 3: gör Butik 2-lagret identiskt med Butik 1  
• Idempotent per order-rad (tabell processed_lines)  
• Extra robust mot decimalsträngar (”2”, ”2.0”, ”2,0”) **överallt**  
• Full verbositet + tydlig logg när rad hoppas över  
• DRY_RUN-läge (export `MODE=DRY_RUN`)  
"""

#######################################################################
# Miljö- & konfig
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
LOOKBACK_HOURS = float(os.getenv("SYNC_LOOKBACK_HOURS", "24"))  # kan vara "2.0"
ALLOW_NEG      = os.getenv("ALLOW_NEGATIVE", "false").lower() == "true"
DRY_RUN        = os.getenv("MODE", "LIVE") == "DRY_RUN"

#######################################################################
# Hjälpfunktioner
#######################################################################
def log(msg: str, lvl: str = "INFO"):
    c = {"INFO": "37", "DEBUG": "36", "WARN": "33", "ERR": "31"}[lvl]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\x1b[{c}m[{now}] {msg}\x1b[0m", flush=True)

_dec_pat = re.compile(r"^-?\d+(?:[.,]\d+)?$")
def to_int(v) -> int:
    """
    Robust konvertering till heltal.
    • Hanterar int, float, Decimal, str(”2.0”), str(”2,0”)
    • Felinmatning → logg & 0
    """
    if v is None:                      return 0
    if isinstance(v, int):             return v
    if isinstance(v, (float, decimal.Decimal)):
        try: return int(round(v))
        except Exception: pass
    if isinstance(v, str) and _dec_pat.match(v.strip()):
        try:  return int(decimal.Decimal(v.replace(",", ".")))
        except Exception: pass
    log(f"⚠️  ogiltigt heltal: {v!r}", "WARN"); return 0

def pretty(obj):               # snygg JSON för logg
    return json.dumps(obj, indent=2, ensure_ascii=False, default=str)

#######################################################################
# Shopify-wrapper
#######################################################################
def shopify(shop: str, tok: str, method: str, path: str,
            params: Optional[dict] = None,
            payload: Optional[dict] = None) -> requests.Response:
    url = f"https://{shop}/admin/api/{API_VERSION}{path}"
    hdr = {"X-Shopify-Access-Token": tok,
           "Accept": "application/json", "Content-Type": "application/json"}
    while True:
        r = requests.request(method, url, headers=hdr,
                             params=params, json=payload, timeout=30)
        if r.status_code == 429:                         # rate-limit
            wait = int(r.headers.get("Retry-After", 2)); time.sleep(wait); continue
        if r.status_code >= 300:
            raise RuntimeError(f"{shop} {method} {path} → {r.status_code}: {r.text}")
        return r

#######################################################################
# Databas (idempotent orderrad-spårning)
#######################################################################
def db(sql: str, vals: tuple = (), fetch=False):
    with psycopg2.connect(DATABASE_URL) as c, c.cursor() as cur:
        cur.execute(sql, vals)
        return cur.fetchall() if fetch else None

def init_db():
    db("""CREATE TABLE IF NOT EXISTS processed_lines (
            shop TEXT NOT NULL,
            line_id BIGINT NOT NULL,
            PRIMARY KEY (shop, line_id)
          );""")

def done(shop: str, lid: int) -> bool:
    return bool(db("SELECT 1 FROM processed_lines WHERE shop=%s AND line_id=%s;",
                   (shop, lid), True))

def mark_done(shop: str, lids: List[int]):
    if not lids: return
    with psycopg2.connect(DATABASE_URL) as c, c.cursor() as cur:
        cur.executemany(
            "INSERT INTO processed_lines (shop,line_id) VALUES (%s,%s) ON CONFLICT DO NOTHING;",
            [(shop, lid) for lid in lids]
        )
    log(f"{shop}: markerade {len(lids)} rader", "DEBUG")

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
    out, path, params = {}, "/products.json", {"limit": 250, "fields": "variants"}
    while True:
        r = shopify(shop, tok, "GET", path, params=params)
        for prod in r.json()["products"]:
            for v in prod["variants"]:
                sku = (v["sku"] or "").strip()
                if sku: out[sku] = (v["inventory_item_id"], v["id"])
        nxt = r.headers.get("Link")
        if nxt and 'rel="next"' in nxt:
            path = nxt.split(";")[0].strip("<>").split(API_VERSION)[1]; params = {}
        else: break
    log(f"{shop}: hittade {len(out)} SKU-varianter", "DEBUG"); return out

def inventory(shop: str, tok: str, loc: int,
              vm: Dict[str, Variant]) -> Dict[str, int]:
    res, ids = {}, [str(i) for i, _ in vm.values()]
    for i in range(0, len(ids), 50):
        chunk = ",".join(ids[i:i+50])
        levels = shopify(shop, tok, "GET", "/inventory_levels.json",
                         params={"inventory_item_ids": chunk, "location_ids": loc}
                         ).json()["inventory_levels"]
        for lv in levels:
            sku = next(k for k, (iid, _) in vm.items() if iid == lv["inventory_item_id"])
            res[sku] = to_int(lv.get("available"))
    log(f"{shop}: lager @loc {loc}: {pretty(res)}", "DEBUG"); return res

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

def adjust(shop: str, tok: str, loc: int, iid: int,
           delta: int, sku: str, before: int, label: str):
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
    out, path, params = [], "/orders.json", {
        "status": "any", "limit": 250, "created_at_min": since.isoformat()}
    while True:
        r = shopify(shop, tok, "GET", path, params=params); out += r.json()["orders"]
        nxt = r.headers.get("Link")
        if nxt and 'rel="next"' in nxt:
            path = nxt.split(";")[0].strip("<>").split(API_VERSION)[1]; params = {}
        else: break
    log(f"{shop}: hämtade {len(out)} ordrar", "DEBUG"); return out

def print_orders(orders: List[dict], label: str):
    for o in orders:
        log(f"{label}: Order {o['id']}  #{o['name']}  skapad {o['created_at']}")
        for li in o.get("line_items", []):
            log(f"   └─ Rad {li['id']}  SKU {li['sku']}  QTY {li['quantity']}")

#######################################################################
# Steg 1 – summera försäljning i Butik 2
#######################################################################
def sales_in_store2(orders: List[dict], vm2: Dict[str, Variant]) -> Dict[str, int]:
    sold, processed = {}, []

    new_orders = [
        o for o in orders
        if any(not done(SHOP_2, to_int(li["id"])) for li in o.get("line_items", []))
    ]
    print_orders(new_orders, "Butik 2 – NYA rader")

    for o in new_orders:
        if o.get("cancelled_at"): continue
        for li in o.get("line_items", []):
            lid = to_int(li["id"])
            if lid == 0: continue
            if done(SHOP_2, lid):
                log(f"⤼ Skippar rad {lid} (redan behandlad)", "DEBUG"); continue
            qty, sku = to_int(li["quantity"]), (li.get("sku") or "").strip()
            if qty <= 0 or sku not in vm2:
                processed.append(lid); continue
            sold[sku] = sold.get(sku, 0) + qty; processed.append(lid)
            log(f"Söld rad {lid}: SKU {sku} +{qty}  (tot {sold[sku]})")
    mark_done(SHOP_2, processed); return sold

#######################################################################
# MAIN
#######################################################################
def main():
    init_db()
    since = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)
    log(f"Ser tillbaka {LOOKBACK_HOURS}h ⇒ {since.isoformat()}", "DEBUG")

    # Grunddata
    loc1, loc2 = primary_location(SHOP_1, TOKEN_1), primary_location(SHOP_2, TOKEN_2)
    vm1, vm2   = variants_by_sku(SHOP_1, TOKEN_1), variants_by_sku(SHOP_2, TOKEN_2)

    # Steg 1
    log("=== STEG 1: Hämta nya orderrader från Butik 2 ===")
    sold2 = sales_in_store2(fetch_orders(SHOP_2, TOKEN_2, since), vm2)
    log(f"Summerad försäljning Butik 2: {pretty(sold2)}")

    # Steg 2
    log("=== STEG 2: Justera Butik 1-lager ===")
    inv1 = inventory(SHOP_1, TOKEN_1, loc1, vm1)
    for sku, qty in sold2.items():
        if sku not in vm1: continue
        iid, vid = vm1[sku]; ensure_trackable(SHOP_1, TOKEN_1, vid); connect(SHOP_1, TOKEN_1, iid, loc1)
        before, delta = inv1.get(sku, 0), -qty
        if before + delta < 0 and not ALLOW_NEG: delta = -before
        adjust(SHOP_1, TOKEN_1, loc1, iid, delta, sku, before, "Butik 1")
        inv1[sku] = before + delta
    log(f"Nytt lager Butik 1: {pretty(inv1)}")

    # Steg 3
    log("=== STEG 3: Synka Butik 2-lager till Butik 1 ===")
    inv2 = inventory(SHOP_2, TOKEN_2, loc2, vm2)
    for sku, qty1 in inv1.items():
        if sku not in vm2: continue
        diff = qty1 - inv2.get(sku, 0)
        if diff == 0: continue
        iid2, vid2 = vm2[sku]; ensure_trackable(SHOP_2, TOKEN_2, vid2); connect(SHOP_2, TOKEN_2, iid2, loc2)
        adjust(SHOP_2, TOKEN_2, loc2, iid2, diff, sku, inv2.get(sku, 0), "Butik 2")
        inv2[sku] = qty1
    log(f"Nytt lager Butik 2: {pretty(inv2)}")
    log("✓ Synk färdig\n")

#######################################################################
if __name__ == "__main__":
    try:
        log(f"=== Shopify-lager-synk startar  [mode={'DRY_RUN' if DRY_RUN else 'LIVE'}] ===")
        main()
    except Exception as e:
        log(f"❌ Fatalt fel: {e}", "ERR"); sys.exit(1)
