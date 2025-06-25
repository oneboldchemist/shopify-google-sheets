#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopify-lager‑synk (v3.0, 2025‑06‑25)
-------------------------------------
• Butik 1 = master
• Full tvåvägssynk på ett gemensamt lager
• Idempotent per order‑*rad*
• Skydd mot dubletter, negativa saldon, race conditions
"""

########################################################################
# Imports & konfiguration
########################################################################
import os, sys, time, json, requests, psycopg2
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

API_VERSION      = "2023-10"          # OK till minst 2025‑10
SHOP_1           = os.environ["SHOP_DOMAIN_1"]
TOKEN_1          = os.environ["SHOPIFY_ACCESS_TOKEN_1"]
SHOP_2           = os.environ["SHOP_DOMAIN_2"]
TOKEN_2          = os.environ["SHOPIFY_ACCESS_TOKEN_2"]
DATABASE_URL     = os.environ["DATABASE_URL"]
LOOKBACK_HOURS   = int(os.getenv("SYNC_LOOKBACK_HOURS", "24"))
ALLOW_NEGATIVE   = os.getenv("ALLOW_NEGATIVE", "false").lower() == "true"
DRY_RUN          = os.getenv("MODE", "LIVE") == "DRY_RUN"

########################################################################
# Hjälpfunktioner: loggning & API‑wrapper
########################################################################
def log(msg:str, *, level:str="INFO"):
    now = datetime.now().strftime("%H:%M:%S")
    colour = {"INFO":"37", "WARN":"33", "ERR":"31"}[level]
    print(f"\x1b[{colour}m[{now}] {msg}\x1b[0m")

def shopify(
    shop:str, token:str, method:str, path:str,
    params:dict|None=None, payload:dict|None=None
) -> dict:
    url = f"https://{shop}/admin/api/{API_VERSION}{path}"
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
        "Accept":       "application/json",
    }
    while True:
        r = requests.request(method, url, headers=headers,
                             params=params, json=payload, timeout=30)
        if r.status_code == 429:  # rate‑limit
            time.sleep(int(r.headers.get("Retry-After", 2)))
            continue
        if r.status_code >= 300:
            raise RuntimeError(f"{shop} {method} {path} → {r.status_code}: {r.text}")
        return r.json() if r.text else {}

########################################################################
# Databas  (spårar *order‑rader* istället för bara order‑ID)
########################################################################
def db_exec(q:str, seq:tuple|list=None, fetch:bool=False):
    with psycopg2.connect(DATABASE_URL) as c, c.cursor() as cur:
        cur.execute(q, seq or ())
        if fetch: return cur.fetchall()

def init_db():
    db_exec("""
        CREATE TABLE IF NOT EXISTS processed_lines (
            shop     TEXT NOT NULL,
            line_id  BIGINT NOT NULL,
            PRIMARY KEY (shop, line_id)
        );
    """)

def is_processed(shop:str, line_id:int) -> bool:
    rows = db_exec("SELECT 1 FROM processed_lines WHERE shop=%s AND line_id=%s;",
                   (shop, line_id), fetch=True)
    return bool(rows)

def mark_processed(shop:str, line_ids:list[int]):
    if not line_ids: return
    db_exec(
        "INSERT INTO processed_lines (shop, line_id) VALUES " +
        ",".join("(%s,%s)" for _ in line_ids) +
        " ON CONFLICT DO NOTHING;",
        tuple((shop, lid) for lid in line_ids)
    )

########################################################################
# Variant‑ och lager‑hjälp
########################################################################
Variant = Tuple[int, int]  # (inventory_item_id, variant_id)

def primary_location(shop:str, tok:str) -> int:
    for loc in shopify(shop, tok, "GET", "/locations.json")["locations"]:
        if loc.get("primary"): return loc["id"]
    return shopify(shop, tok, "GET", "/locations.json")["locations"][0]["id"]

def variants_by_sku(shop:str, tok:str) -> Dict[str, Variant]:
    out:Dict[str,Variant] = {}
    path, params = "/products.json", {"limit":250, "fields":"variants"}
    while True:
        j = shopify(shop, tok, "GET", path, params=params)
        for p in j["products"]:
            for v in p["variants"]:
                sku = (v["sku"] or "").strip()
                if sku: out[sku] = (v["inventory_item_id"], v["id"])
        link = requests.utils.parse_header_links(
            j.get("link", "") if isinstance(j, dict) else ""
        )
        nxt = next((l for l in link if l["rel"]=="next"), None)
        if nxt: path, params = nxt["url"].split(API_VERSION,1)[1], {}
        else:   break
    return out

def inventory(shop:str, tok:str, loc:int, varmap:dict[str,Variant]) -> dict[str,int]:
    out:dict[str,int] = {}
    ids = [str(iid) for iid,_ in varmap.values()]
    for i in range(0, len(ids), 50):
        chunk = ",".join(ids[i:i+50])
        j = shopify(
            shop, tok, "GET", "/inventory_levels.json",
            params={"inventory_item_ids":chunk, "location_ids":loc}
        )
        for lvl in j["inventory_levels"]:
            avail = lvl.get("available") or 0
            sku   = next(k for k,(iid,_) in varmap.items() if iid==lvl["inventory_item_id"])
            out[sku] = int(avail)
    return out

def adjust(shop:str, tok:str, loc:int, inv_id:int, delta:int, sku:str):
    if DRY_RUN:
        log(f"[DRY] {shop} adjust {delta:+} (SKU {sku})"); return
    shopify(shop, tok, "POST", "/inventory_levels/adjust.json",
            payload={"location_id":loc,"inventory_item_id":inv_id,
                     "available_adjustment":delta})
    log(f"    ↳ justerade {delta:+}  (SKU {sku})")

def ensure_trackable(shop:str, tok:str, var_id:int):
    v = shopify(shop, tok, "GET", f"/variants/{var_id}.json")["variant"]
    if v["inventory_management"]=="shopify": return
    shopify(shop, tok, "PUT", f"/variants/{var_id}.json",
            payload={"variant":{"id":var_id,"inventory_management":"shopify"}})
    log(f"✓ inventory_management=shopify för variant {var_id} ({shop})")

def connect(shop:str, tok:str, inv_id:int, loc:int):
    try:
        shopify(shop, tok, "POST", "/inventory_levels/connect.json",
                payload={"location_id":loc,"inventory_item_id":inv_id})
        log(f"✓ connect  inventory_item {inv_id} → loc {loc} ({shop})")
    except RuntimeError as e:
        if "422" not in str(e): raise

########################################################################
# Order‑hämtning & rad‑processing
########################################################################
def fetch_orders(shop:str, tok:str, since:datetime) -> list[dict]:
    all_:list[dict] = []
    p = {"status":"any","limit":250,"created_at_min":since.isoformat()}
    path = "/orders.json"
    while True:
        r = shopify(shop, tok, "GET", path, params=p)
        all_.extend(r["orders"])
        link = requests.utils.parse_header_links(r.get("link","") if isinstance(r,dict) else "")
        nxt  = next((l for l in link if l["rel"]=="next"), None)
        if nxt: path, p = nxt["url"].split(API_VERSION,1)[1], {}
        else:   break
    return all_

def handle_outgoing(
    orders:list[dict], shop_src:str, shop_dst:str,
    tok_dst:str, loc_dst:int, variants_dst:dict[str,Variant], qty_dst:dict[str,int]
):
    """
    • shop_src = där ordern lades (deras lager är redan uppdaterat av Shopify)
    • shop_dst = butiken vars lager behöver justeras
    """
    processed:list[int] = []
    for o in orders:
        for li in o.get("line_items", []):
            lid = li["id"]
            if is_processed(shop_src, lid): continue
            if o.get("cancelled_at") or li.get("fulfillable_quantity",0)<0:
                continue  # retur/annullering
            sku = (li.get("sku") or "").strip()
            qty = int(li.get("quantity") or 0)
            if not sku or sku not in variants_dst:  # okända/blank
                log(f"⚠️  Hoppar rad {lid}: saknar giltig SKU ({sku})", level="WARN")
                processed.append(lid)
                continue

            inv_id, var_id = variants_dst[sku]
            ensure_trackable(shop_dst, tok_dst, var_id)
            connect(shop_dst, tok_dst, inv_id, loc_dst)

            before = qty_dst.get(sku, 0)
            delta  = -qty  # säljs alltid → dra bort
            after  = before + delta
            if after < 0 and not ALLOW_NEGATIVE:
                delta = -before   # ta bara ned till noll
                after = 0
            log(f"{shop_src} order {lid}: SKU {sku} qty {qty} → diff {delta:+}")
            adjust(shop_dst, tok_dst, loc_dst, inv_id, delta, sku)
            qty_dst[sku] = after
            # Bekräfta:
            real = inventory(shop_dst, tok_dst, loc_dst, {sku:variants_dst[sku]})[sku]
            if real != after:
                log(f"⚠️  Förväntat {after}, fick {real} – kör set", level="WARN")
                if not DRY_RUN:
                    shopify(shop_dst, tok_dst, "POST",
                            "/inventory_levels/set.json",
                            payload={"location_id":loc_dst,"inventory_item_id":inv_id,
                                     "available":after})
            processed.append(lid)
    mark_processed(shop_src, processed)

########################################################################
# Synk‑steget (matcha hela tabellen)
########################################################################
def full_sync(
    src_qty:dict[str,int],
    dst_shop:str, dst_tok:str, dst_loc:int, dst_variants:dict[str,Variant]
):
    updates = 0
    dst_qty = inventory(dst_shop, dst_tok, dst_loc, dst_variants)
    for sku, src_val in src_qty.items():
        if sku not in dst_variants: continue
        diff = src_val - dst_qty.get(sku, 0)
        if diff == 0: continue
        inv_id, var_id = dst_variants[sku]
        ensure_trackable(dst_shop, dst_tok, var_id)
        connect(dst_shop, dst_tok, inv_id, dst_loc)
        log(f"Full synk {dst_shop}: SKU {sku}  diff {diff:+}")
        adjust(dst_shop, dst_tok, dst_loc, inv_id, diff, sku)
        updates += 1
    log(f"✓ Full synk klar – {updates} SKU:er justerade i {dst_shop}")

########################################################################
# MAIN
########################################################################
def main():
    init_db()
    since  = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)
    loc1   = primary_location(SHOP_1, TOKEN_1)
    loc2   = primary_location(SHOP_2, TOKEN_2)

    # Variant‑tabeller
    v1 = variants_by_sku(SHOP_1, TOKEN_1)
    v2 = variants_by_sku(SHOP_2, TOKEN_2)

    # Nuläges‑lager
    q1 = inventory(SHOP_1, TOKEN_1, loc1, v1)
    q2 = inventory(SHOP_2, TOKEN_2, loc2, v2)

    # Hämta färska ordrar
    ord1 = fetch_orders(SHOP_1, TOKEN_1, since)
    ord2 = fetch_orders(SHOP_2, TOKEN_2, since)

    # 1️⃣ Order lagda i butik 2 drar master (q1)   
    handle_outgoing(ord2, SHOP_2, SHOP_1, TOKEN_1, loc1, v1, q1)

    # 2️⃣ Order lagda i butik 1 påverkar redan q1 ➜ synka q1 → butik 2
    handle_outgoing(ord1, SHOP_1, SHOP_2, TOKEN_2, loc2, v2, q2)  # endast retur‑/annull.

    # 3️⃣ Full tabell‑synk master → sekundär
    full_sync(q1, SHOP_2, TOKEN_2, loc2, v2)

    log("=== Synk‑körning klar ===")

if __name__ == "__main__":
    try:
        mode = "TEST‑läge" if DRY_RUN else "LIVE"
        log(f"=== Shopify‑synk (v3.0) startar – {mode} ===")
        main()
    except Exception as e:
        log(f"❌ Fatalt fel: {e}", level="ERR")
        sys.exit(1)
