#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
shopify_sync_offset.py  (2025-06-25)
====================================
• Butik 1 = masterlager
• Först dras alla NYA försäljningar i Butik 2 av från Butik 1
• Därefter synkas hela lagret så att Butik 2 == Butik 1
• Idempotent spårning av orderrader i Postgres
• DRY_RUN-läge: exportera MODE=DRY_RUN för att skriva logg utan att röra lagret
"""

#######################################################################
# 1. Imports & konfiguration
#######################################################################
import os, time, requests, psycopg2, decimal, re, sys
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

API_VERSION    = "2023-10"                   # Shopify REST-version
SHOP_1         = os.environ["SHOP_DOMAIN_1"]     # butik 1 (master)
TOKEN_1        = os.environ["SHOPIFY_ACCESS_TOKEN_1"]
SHOP_2         = os.environ["SHOP_DOMAIN_2"]     # butik 2 (sekundär)
TOKEN_2        = os.environ["SHOPIFY_ACCESS_TOKEN_2"]
DATABASE_URL   = os.environ["DATABASE_URL"]      # ex. postgres://user:pass@host/db
LOOKBACK_HOURS = int(os.getenv("SYNC_LOOKBACK_HOURS", "24"))
ALLOW_NEG      = os.getenv("ALLOW_NEGATIVE", "false").lower() == "true"
DRY_RUN        = os.getenv("MODE", "LIVE").upper() == "DRY_RUN"

#######################################################################
# 2. Små hjälpare
#######################################################################
def log(msg: str, *, lvl: str = "INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    clr = {"INFO":"37","OK":"32","WARN":"33","ERR":"31"}[lvl]
    print(f"\x1b[{clr}m[{ts}] {msg}\x1b[0m")

_dec_re = re.compile(r"^-?\d+(?:[.,]\d+)?$")
def to_int(x) -> int:
    """Konverterar diverse taltyper/strängar → int."""
    if x is None: return 0
    if isinstance(x,int): return x
    if isinstance(x,(float,decimal.Decimal)): return int(round(x))
    if isinstance(x,str) and _dec_re.match(x.strip()):
        return int(decimal.Decimal(x.replace(",", ".")))
    log(f"⚠️ ogiltigt tal: {x!r}", lvl="WARN"); return 0

#######################################################################
# 3. Minimal Shopify-wrapper
#######################################################################
def shopify(shop:str, token:str, method:str, path:str,
            params:dict|None=None, json:dict|None=None) -> requests.Response:
    url = f"https://{shop}/admin/api/{API_VERSION}{path}"
    hdr = {"X-Shopify-Access-Token": token,
           "Content-Type":"application/json", "Accept":"application/json"}
    while True:
        r = requests.request(method, url, headers=hdr, params=params, json=json, timeout=30)
        if r.status_code == 429:                       # rate-limit
            time.sleep(int(r.headers.get("Retry-After","2"))); continue
        if r.status_code >= 300:
            raise RuntimeError(f"{shop} {method} {path} → {r.status_code}: {r.text[:200]}")
        return r

#######################################################################
# 4. Postgres för idempotens (order-rader)
#######################################################################
def db(q:str, vals:tuple=(), *, fetch=False):
    with psycopg2.connect(DATABASE_URL) as c, c.cursor() as cur:
        cur.execute(q, vals)
        if fetch: return cur.fetchall()

def init_db():
    db("""CREATE TABLE IF NOT EXISTS processed_lines (
             shop TEXT NOT NULL,
             line_id BIGINT NOT NULL,
             PRIMARY KEY (shop, line_id)
          );""")

def done(shop:str, lid:int) -> bool:
    return bool(db("SELECT 1 FROM processed_lines WHERE shop=%s AND line_id=%s;",
                   (shop,lid), fetch=True))

def mark(shop:str, lids:List[int]):
    if not lids: return
    with psycopg2.connect(DATABASE_URL) as c, c.cursor() as cur:
        cur.executemany(
            "INSERT INTO processed_lines (shop,line_id) VALUES (%s,%s) ON CONFLICT DO NOTHING;",
            [(shop,l) for l in lids])

#######################################################################
# 5. Lager-verktyg
#######################################################################
Variant  = Tuple[int,int]      # (inventory_item_id, variant_id)

def primary_location(shop:str, tok:str) -> int:
    locs = shopify(shop,tok,"GET","/locations.json").json()["locations"]
    for l in locs:
        if l.get("primary"): return l["id"]
    return locs[0]["id"]

def variants_by_sku(shop:str, tok:str) -> Dict[str,Variant]:
    out:Dict[str,Variant] = {}
    path, params = "/products.json", {"limit":250,"fields":"variants"}
    while True:
        r = shopify(shop,tok,"GET",path,params=params)
        for p in r.json()["products"]:
            for v in p["variants"]:
                sku = (v["sku"] or "").strip()
                if sku: out[sku]=(v["inventory_item_id"], v["id"])
        link = r.headers.get("Link")
        if link and 'rel="next"' in link:
            path = link.split(";")[0].strip("<>").split(API_VERSION)[1]; params={}
        else: break
    return out

def inventory(shop:str, tok:str, loc:int, vmap:Dict[str,Variant]) -> Dict[str,int]:
    qty:Dict[str,int] = {}
    ids = [str(iid) for iid,_ in vmap.values()]
    for i in range(0,len(ids),50):
        levels = shopify(
            shop,tok,"GET","/inventory_levels.json",
            params={"inventory_item_ids":",".join(ids[i:i+50]),
                    "location_ids":loc}).json()["inventory_levels"]
        for lvl in levels:
            avail = to_int(lvl["available"])
            sku = next(k for k,(iid,_) in vmap.items() if iid==lvl["inventory_item_id"])
            qty[sku]=avail
    return qty

def ensure_trackable(shop:str,tok:str,vid:int):
    v = shopify(shop,tok,"GET",f"/variants/{vid}.json").json()["variant"]
    if v["inventory_management"]=="shopify": return
    if DRY_RUN: return
    shopify(shop,tok,"PUT",f"/variants/{vid}.json",
            json={"variant":{"id":vid,"inventory_management":"shopify"}})

def connect(shop:str,tok:str,iid:int,loc:int):
    try:
        if DRY_RUN: return
        shopify(shop,tok,"POST","/inventory_levels/connect.json",
                json={"location_id":loc,"inventory_item_id":iid})
    except RuntimeError as e:
        if "422" not in str(e): raise

def adjust(shop:str,tok:str,loc:int,iid:int,delta:int,sku:str):
    if delta==0: return
    if DRY_RUN:
        log(f"[DRY] {shop}: {sku} {delta:+}", lvl="OK"); return
    shopify(shop,tok,"POST","/inventory_levels/adjust.json",
            json={"location_id":loc,"inventory_item_id":iid,
                  "available_adjustment":delta})
    log(f"✓ {shop} {sku} {delta:+}", lvl="OK")

#######################################################################
# 6. Hämta NYA försäljningar i butik 2 och dra av dem i butik 1
#######################################################################
def offset_sales_from_store2(
    since:datetime,
    vmap1:Dict[str,Variant], loc1:int,
    vmap2:Dict[str,Variant], tok2:str
) -> None:
    orders = []
    path, params = "/orders.json", {"status":"any","limit":250,"created_at_min":since.isoformat()}
    while True:
        r = shopify(SHOP_2,tok2,"GET",path,params=params)
        orders.extend(r.json()["orders"])
        link = r.headers.get("Link")
        if link and 'rel="next"' in link:
            path = link.split(";")[0].strip("<>").split(API_VERSION)[1]; params={}
        else: break

    loc1_tok = TOKEN_1
    loc1_shop = SHOP_1
    loc1_id   = loc1
    qty1      = inventory(SHOP_1,TOKEN_1,loc1_id,vmap1)  # lokalt minne (slipper ny GET per rad)

    processed:List[int]=[]
    for o in orders:
        if o.get("cancelled_at"): continue
        for li in o.get("line_items",[]):
            lid = to_int(li["id"])
            if lid==0 or done(SHOP_2,lid): continue
            sku = (li.get("sku") or "").strip()
            if not sku or sku not in vmap1:
                processed.append(lid); continue

            sold = to_int(li["quantity"])
            iid1, vid1 = vmap1[sku]

            # säkerställ spårbar & connected
            ensure_trackable(SHOP_1,TOKEN_1,vid1)
            connect(SHOP_1,TOKEN_1,iid1,loc1_id)

            before = qty1.get(sku,0)
            delta  = -sold
            after  = before + delta
            if after < 0 and not ALLOW_NEG:
                delta = -before; after = 0
                log(f"⚠️  Justering kapad (negativt lager ej tillåtet) SKU {sku}", lvl="WARN")

            log(f"Sales offset: SKU {sku} {sold} st (order 2) → lager 1 {delta:+}")
            adjust(SHOP_1,TOKEN_1,loc1_id,iid1,delta,sku)
            qty1[sku]=after
            processed.append(lid)

    mark(SHOP_2,processed)

#######################################################################
# 7. Full synk Lager 1 → Lager 2
#######################################################################
def full_sync_to_store2(
    vmap1:Dict[str,Variant], loc1:int,
    vmap2:Dict[str,Variant], loc2:int
):
    qty1 = inventory(SHOP_1,TOKEN_1,loc1,vmap1)
    qty2 = inventory(SHOP_2,TOKEN_2,loc2,vmap2)
    updates = 0
    for sku,val1 in qty1.items():
        if sku not in vmap2: continue
        diff = val1 - qty2.get(sku,0)
        if diff==0: continue
        iid2, vid2 = vmap2[sku]
        ensure_trackable(SHOP_2,TOKEN_2,vid2)
        connect(SHOP_2,TOKEN_2,iid2,loc2)
        log(f"Synk: SKU {sku} diff {diff:+}")
        adjust(SHOP_2,TOKEN_2,loc2,iid2,diff,sku)
        updates += 1
    log(f"✓ Full synk klar – {updates} SKU:er uppdaterade", lvl="OK")

#######################################################################
# 8. MAIN
#######################################################################
def main():
    init_db()
    since = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)

    loc1 = primary_location(SHOP_1,TOKEN_1)
    loc2 = primary_location(SHOP_2,TOKEN_2)

    vmap1 = variants_by_sku(SHOP_1,TOKEN_1)
    vmap2 = variants_by_sku(SHOP_2,TOKEN_2)

    # 1) Dra av nya försäljningar i Butik 2 från Butik 1
    offset_sales_from_store2(since, vmap1, loc1, vmap2, TOKEN_2)

    # 2) Synka så att Butik 2 får exakt samma lager som uppdaterade Butik 1
    full_sync_to_store2(vmap1, loc1, vmap2, loc2)

    log("=== KLAR ===", lvl="OK")

if __name__ == "__main__":
    try:
        log(f"Startar lager-synk   DRY_RUN={DRY_RUN}", lvl="INFO")
        main()
    except Exception as e:
        log(f"❌ Fatalt fel: {e}", lvl="ERR")
        sys.exit(1)
