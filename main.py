#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OBC – Lager & Försäljning  (ENDAST Store 1)

• Synkar alltid lagersaldo för angiven Shopify-location (inte USA-lagret).
• Startdatum för orderhämtning: 2025-07-09 00:00 UTC.
• RESET_DATABASE=true kan nollställa tabellerna vid behov.
"""
# --------------------------------------------------------------------------- #
import os, re, time, json, math, requests, gspread, psycopg2
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from typing import Dict, List
# --------------------------------------------------------------------------- #
#                              Miljövariabler                                 #
# --------------------------------------------------------------------------- #
SHOP_DOMAIN       = os.getenv("SHOP_DOMAIN_1") or "first-shop.myshopify.com"
SHOPIFY_TOKEN     = os.getenv("SHOPIFY_ACCESS_TOKEN_1") or "access-token-shop-1"
SHOP_LOCATION_ID  = os.getenv("SHOP_LOCATION_ID")          # ***MÅSTE anges***
RESET_DB          = (os.getenv("RESET_DATABASE", "false").lower() == "true")

if not SHOP_LOCATION_ID:
    raise ValueError("Miljövariabel SHOP_LOCATION_ID saknas – vilket lager ska användas?")

GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not GOOGLE_CREDS_JSON:
    raise ValueError("Missing env GOOGLE_CREDENTIALS_JSON")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("Missing env DATABASE_URL")

SCOPES = ["https://spreadsheets.google.com/feeds",
          "https://www.googleapis.com/auth/drive"]
creds  = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_CREDS_JSON), SCOPES)
client = gspread.authorize(creds)

# Google Sheets-objekt
sheet          = client.open("OBC lager").sheet1           # Blad 1
sales_sheet    = client.open("OBC lager").worksheet("Blad2")
sales_sheet_US = client.open("OBC lager").worksheet("Blad3")
# --------------------------------------------------------------------------- #
#                               PostgreSQL                                    #
# --------------------------------------------------------------------------- #
def pg_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def init_tables() -> None:
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_orders (
                order_id TEXT PRIMARY KEY,
                processed_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """)
        conn.commit()

def reset_database() -> None:
    if not RESET_DB:
        return
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS processed_orders;")
        conn.commit()
    print("[DB] Tabeller rensade (RESET_DATABASE=true).")

def processed_order_ids() -> set:
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT order_id FROM processed_orders;")
        return {row[0] for row in cur.fetchall()}

def save_processed(ids: List[str]) -> None:
    if not ids:
        return
    with pg_conn() as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO processed_orders (order_id) VALUES (%s) ON CONFLICT DO NOTHING;",
            [(oid,) for oid in ids],
        )
        conn.commit()
# --------------------------------------------------------------------------- #
#                               Hjälpfunktioner                               #
# --------------------------------------------------------------------------- #
def safe_api_call(func, *args, **kwargs):
    try:
        res = func(*args, **kwargs)
        time.sleep(2)
        return res
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 429:
            print("[Google] 429 – väntar 60 s …")
            time.sleep(60)
            return safe_api_call(func, *args, **kwargs)
        raise

def extract_perfume_number(text: str):
    m = re.search(r"(\d{1,3}(?:\.\d+)?)\b", text)
    return float(m.group(1)) if m else None

def fmt_perfume(num: float) -> str:
    return str(int(num)) if math.isclose(num, round(num)) else str(num)
# --------------------------------------------------------------------------- #
#                    Shopify → lager (endast en location)                     #
# --------------------------------------------------------------------------- #
def fetch_shopify_inventory(domain: str, token: str, location_id: str) -> Dict[float, int]:
    base_v  = f"https://{domain}/admin/api/2023-07/variants.json"
    headers = {"X-Shopify-Access-Token": token}
    params  = {"limit": 250, "fields": "id,title,sku,inventory_item_id"}
    item_to_perf: Dict[int, float] = {}
    while True:
        r = safe_api_call(requests.get, base_v, headers=headers, params=params)
        if r.status_code != 200:
            raise RuntimeError(f"Shopify variants error {r.status_code}: {r.text}")
        for v in r.json().get("variants", []):
            pnr = extract_perfume_number(v.get("sku") or v.get("title", ""))
            if pnr is not None:
                item_to_perf[v["inventory_item_id"]] = pnr
        next_url = None
        for part in r.headers.get("Link", "").split(","):
            if 'rel="next"' in part:
                next_url = part[part.find("<")+1:part.find(">")]
                break
        if not next_url:
            break
        base_v, params = next_url, {}
    if not item_to_perf:
        return {}

    inventory: Dict[float, int] = {}
    items = list(item_to_perf.keys())
    for i in range(0, len(items), 50):
        sub = ",".join(map(str, items[i:i+50]))
        url = f"https://{domain}/admin/api/2023-07/inventory_levels.json"
        level_params = {
            "inventory_item_ids": sub,
            "location_ids": location_id,
            "limit": 250,
        }
        r = safe_api_call(requests.get, url, headers=headers, params=level_params)
        if r.status_code != 200:
            raise RuntimeError(f"Shopify inventory_levels error {r.status_code}: {r.text}")
        for lv in r.json().get("inventory_levels", []):
            p = item_to_perf.get(lv["inventory_item_id"])
            if p is None:
                continue
            avail = lv.get("available") or 0
            inventory[p] = inventory.get(p, 0) + int(avail)
    print(f"[Lager-sync] Hämtade {len(inventory)} parfymnummer från Shopify location {location_id}.")
    return inventory

def write_inventory_to_sheet(inv: Dict[float, int]) -> None:
    vals     = safe_api_call(sheet.get_all_values)
    p_to_row = {}
    for i, r in enumerate(vals, 1):
        if i == 1 or not r or not r[0]:
            continue
        try:
            p_to_row[float(r[0])] = i
        except ValueError:
            continue

    new_rows, updates = [], []
    for pnum, qty in inv.items():
        if pnum in p_to_row:
            updates.append(gspread.Cell(p_to_row[pnum], 2, qty))
        else:
            new_rows.append([fmt_perfume(pnum), qty, 0])

    if new_rows:
        safe_api_call(sheet.append_rows, new_rows, value_input_option="USER_ENTERED")
        print(f"[Lager-sync] Lagt till {len(new_rows)} nya rader.")
    if updates:
        safe_api_call(sheet.update_cells, updates)
        print(f"[Lager-sync] Uppdaterat lagersaldo för {len(updates)} rader.")
# --------------------------------------------------------------------------- #
#                Robust avläsning av “Sold:” (tål tomma rubriker)             #
# --------------------------------------------------------------------------- #
def read_sold_column() -> Dict[float, int]:
    rows = safe_api_call(sheet.get_all_values)
    if not rows:
        return {}

    header = [h.strip().lower() for h in rows[0]]
    # hitta kolumnerna – ignorera kolon, versaler och blanksteg
    def col_index(name: str):
        for i, h in enumerate(header):
            h_clean = h.replace(":", "").strip()
            if h_clean == name.lower():
                return i
        return None

    idx_num  = col_index("nummer")
    idx_sold = col_index("sold")
    if idx_num is None or idx_sold is None:
        raise RuntimeError("Hittar inte kolumnerna 'nummer:' och 'Sold:' på rad 1.")

    sold: Dict[float, int] = {}
    for r in rows[1:]:
        if len(r) <= max(idx_num, idx_sold):
            continue
        try:
            num = float(r[idx_num].strip())
            qty = int((r[idx_sold] or "0").replace("−", "-").strip())
            sold[num] = qty
        except ValueError:
            continue
    return sold
# --------------------------------------------------------------------------- #
#                       Shopify → nya ordrar osv …                            #
# --------------------------------------------------------------------------- #
def fetch_new_orders(domain: str, token: str, start_date: datetime):
    base     = f"https://{domain}/admin/api/2023-07/orders.json"
    headers  = {"X-Shopify-Access-Token": token}
    params   = {"created_at_min": start_date.isoformat(),
                "limit": 250, "status": "any"}
    orders = []
    while True:
        r = safe_api_call(requests.get, base, headers=headers, params=params)
        if r.status_code != 200:
            raise RuntimeError(f"Shopify orders error {r.status_code}: {r.text}")
        orders.extend(r.json().get("orders", []))
        next_url = None
        for part in r.headers.get("Link", "").split(","):
            if 'rel="next"' in part:
                next_url = part[part.find("<")+1:part.find(">")]
                break
        if not next_url:
            break
        base, params = next_url, {}
    print(f"[Orders] Hämtade totalt {len(orders)} ordrar.")
    return orders

def process_orders(orders, sold: Dict[float, int], processed: set):
    new_ids, sales_log, sales_log_US = [], {}, {}
    for o in orders:
        oid = str(o["id"])
        if oid in processed:
            continue
        date_str = datetime.strptime(
            o["created_at"], "%Y-%m-%dT%H:%M:%S%z"
        ).date().isoformat()
        is_US = (o.get("shipping_address") or {}).get("country_code") == "US"

        def add_sale(pnum: float, qty: int):
            sold[pnum] = sold.get(pnum, 0) + qty
            sales_log    .setdefault(date_str, {}).setdefault(pnum, 0)
            sales_log[date_str][pnum] += qty
            if is_US:
                sales_log_US.setdefault(date_str, {}).setdefault(pnum, 0)
                sales_log_US[date_str][pnum] += qty

        for item in o["line_items"]:
            title, qty = item["title"], item["quantity"]
            if "sample" in title.lower():
                continue
            if "Fragrance Bundle" in title:
                for prop in item["properties"]:
                    p = extract_perfume_number(prop["value"])
                    if p is not None:
                        add_sale(p, qty)
            else:
                p = extract_perfume_number(title)
                if p is not None:
                    add_sale(p, qty)
        new_ids.append(oid)
    return new_ids, sales_log, sales_log_US

def ensure_column(sheet_obj, pnum: float) -> int:
    headers = safe_api_call(sheet_obj.row_values, 1)
    label   = fmt_perfume(pnum)
    if label in headers:
        return headers.index(label) + 1
    safe_api_call(sheet_obj.add_cols, 1)
    col = len(headers) + 1
    safe_api_call(sheet_obj.update_cell, 1, col, label)
    return col

def ensure_row(sheet_obj, date_str: str, current_vals: List[List[str]]) -> int:
    dates = [r[0] for r in current_vals]
    if date_str in dates:
        return dates.index(date_str) + 1
    insert_at = sorted(dates[1:] + [date_str]).index(date_str) + 2
    safe_api_call(sheet_obj.insert_row, [date_str], insert_at)
    return insert_at

def log_sales(log: Dict[str, Dict[float, int]], sheet_obj):
    if not log:
        return
    vals, updates = safe_api_call(sheet_obj.get_all_values), []
    for d, fdict in sorted(log.items()):
        row = ensure_row(sheet_obj, d, vals)
        for p, q in fdict.items():
            col = ensure_column(sheet_obj, p)
            cur_val = safe_api_call(sheet_obj.cell, row, col).value or "0"
            new_val = int(cur_val) + q
            updates.append(gspread.Cell(row, col, new_val))
    if updates:
        safe_api_call(sheet_obj.update_cells, updates)

def update_7d_average():
    sales_data = safe_api_call(sales_sheet.get_all_values)
    if len(sales_data) < 2:
        return
    headers = sales_data[0]
    today   = datetime.utcnow().date()
    win_set = {today - timedelta(d) for d in range(7)}
    sums: Dict[float, int] = {}

    for row in sales_data[1:]:
        if not row or not row[0]:
            continue
        try:
            date_obj = datetime.strptime(row[0], "%Y-%m-%d").date()
        except ValueError:
            continue
        if date_obj not in win_set:
            continue
        for idx, header in enumerate(headers[1:], 1):
            try:
                pnum = float(header)
                qty  = int(row[idx] or 0)
            except ValueError:
                continue
            sums[pnum] = sums.get(pnum, 0) + qty

    if not sums:
        return
    blad1_vals = safe_api_call(sheet.get_all_values)
    p_to_row   = {float(r[0]): i for i, r in enumerate(blad1_vals, 1)
                  if i > 1 and r and r[0].strip()}
    cells = [
        gspread.Cell(p_to_row[p], 4, round(total/7, 2))
        for p, total in sums.items() if p in p_to_row
    ]
    if cells:
        safe_api_call(sheet.update_cell, 1, 4, "Snitt 7d (per dag)")
        safe_api_call(sheet.update_cells, cells)
# --------------------------------------------------------------------------- #
def main():
    print("=== OBC Lager-script (Endast Store 1) ===")
    reset_database()
    init_tables()

    inventory = fetch_shopify_inventory(SHOP_DOMAIN, SHOPIFY_TOKEN, SHOP_LOCATION_ID)
    write_inventory_to_sheet(inventory)

    sold      = read_sold_column()
    processed = processed_order_ids()

    START_DATE = datetime(2025, 7, 9)
    print(f"[Orders] Hämtar ordrar från {START_DATE.date()} …")
    orders = fetch_new_orders(SHOP_DOMAIN, SHOPIFY_TOKEN, START_DATE)

    new_ids, sales_log, sales_log_US = process_orders(orders, sold, processed)
    save_processed(new_ids)

    log_sales(sales_log,    sales_sheet)
    log_sales(sales_log_US, sales_sheet_US)

    blad1_vals = safe_api_call(sheet.get_all_values)
    p_to_row   = {float(r[0]): i for i, r in enumerate(blad1_vals, 1)
                  if i > 1 and r and r[0].strip()}
    sold_cells = [gspread.Cell(p_to_row[p], 3, q) for p, q in sold.items() if p in p_to_row]
    if sold_cells:
        safe_api_call(sheet.update_cells, sold_cells)

    update_7d_average()
    print("✔ Klart", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "UTC")
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    main()
