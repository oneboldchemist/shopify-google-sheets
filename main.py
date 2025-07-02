#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OBC – Lager & Försäljning  (ENDAST Store 1)

• Läser butik 1:s ordrar, aldrig butik 2.
• Första körningen synkar lagersaldot Shopify → Google Sheets.
• Därefter kör skriptet samma rutin vid varje körning:
    – hämtar nya ordrar (från 2025-07-02 00:00 UTC och framåt)
    – drar av lager & ökar “Sold:” i Blad 1
    – loggar daglig försäljning i Blad 2 (alla) & Blad 3 (USA)
    – beräknar rullande 7-dagars snitt till kolumn D i Blad 1
    – sparar processade order-ID i Postgres

Skrip­tet gör inga skrivande anrop till Shopify.
"""
# --------------------------------------------------------------------------- #
import os, re, time, json, requests, gspread, psycopg2
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from typing import Dict, List
# --------------------------------------------------------------------------- #
#                        Miljövariabler & API-autentisering                  #
# --------------------------------------------------------------------------- #
SHOP_DOMAIN = os.getenv("SHOP_DOMAIN_1") or "first-shop.myshopify.com"
SHOPIFY_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN_1") or "access-token-shop-1"

GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not GOOGLE_CREDS_JSON:
    raise ValueError("Missing env GOOGLE_CREDENTIALS_JSON")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("Missing env DATABASE_URL")

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_dict(
    json.loads(GOOGLE_CREDS_JSON), SCOPES
)
client = gspread.authorize(creds)

# Google Sheets
sheet          = client.open("OBC lager").sheet1           # Blad 1
sales_sheet    = client.open("OBC lager").worksheet("Blad2")
sales_sheet_US = client.open("OBC lager").worksheet("Blad3")

# --------------------------------------------------------------------------- #
#                              PostgreSQL-hjälpare                            #
# --------------------------------------------------------------------------- #
def pg_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def init_tables() -> None:
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_orders (
                order_id TEXT PRIMARY KEY,
                processed_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS inventory_initialized (
                done BOOLEAN PRIMARY KEY
            );
            """
        )
        conn.commit()

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

def inventory_initialized() -> bool:
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT done FROM inventory_initialized WHERE done = TRUE;")
        return cur.fetchone() is not None

def mark_inventory_initialized() -> None:
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO inventory_initialized (done) VALUES (TRUE) ON CONFLICT DO NOTHING;"
        )
        conn.commit()

# --------------------------------------------------------------------------- #
#                               Övriga hjälpare                               #
# --------------------------------------------------------------------------- #
def safe_api_call(func, *args, **kwargs):
    """Pausar 2 s mellan anrop och back-off:ar 60 s vid Google 429."""
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
    return str(int(num)) if num.is_integer() else str(num)

# --------------------------------------------------------------------------- #
#                       Engångs-synk – Shopify → Blad 1                       #
# --------------------------------------------------------------------------- #
def fetch_shopify_inventory(domain: str, token: str) -> Dict[float, int]:
    base = f"https://{domain}/admin/api/2023-07/variants.json"
    params = {"limit": 250, "fields": "id,title,sku,inventory_quantity"}
    headers = {"X-Shopify-Access-Token": token}

    inventory: Dict[float, int] = {}
    while True:
        r = safe_api_call(requests.get, base, headers=headers, params=params)
        if r.status_code != 200:
            raise RuntimeError(f"Shopify variants error {r.status_code}: {r.text}")
        for v in r.json().get("variants", []):
            text = v.get("sku") or v.get("title", "")
            pnr = extract_perfume_number(text)
            if pnr is None:
                continue
            inventory[pnr] = inventory.get(pnr, 0) + int(v["inventory_quantity"])
        # pagination
        link = r.headers.get("Link", "")
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                next_url = part[part.find("<") + 1 : part.find(">")]
                break
        if not next_url:
            break
        base, params = next_url, {}
    print(f"[Init-lager] Hämtade {len(inventory)} parfymnummer från Shopify.")
    return inventory

def write_inventory_to_sheet(inv: Dict[float, int]) -> None:
    vals = safe_api_call(sheet.get_all_values)
    header = vals[0] if vals else []
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
            updates.append(gspread.Cell(p_to_row[pnum], 2, qty))  # kol B
        else:
            new_rows.append([fmt_perfume(pnum), qty, 0])

    if new_rows:
        safe_api_call(sheet.append_rows, new_rows, value_input_option="USER_ENTERED")
        print(f"[Init-lager] Lagt till {len(new_rows)} nya rader.")
    if updates:
        safe_api_call(sheet.update_cells, updates)
        print(f"[Init-lager] Uppdaterat lagersaldo för {len(updates)} rader.")

# --------------------------------------------------------------------------- #
#                        Läser lager & Sold från Blad 1                       #
# --------------------------------------------------------------------------- #
def read_inventory_and_sold():
    recs = safe_api_call(
        sheet.get_all_records, expected_headers=["nummer:", "Antal:", "Sold:"]
    )
    inv, sold = {}, {}
    for r in recs:
        try:
            num = float(r["nummer:"])
            inv[num] = int(str(r["Antal:"]).replace("−", "-").strip()) if r["Antal:"] else 0
            sold[num] = int(str(r["Sold:"]).strip()) if r["Sold:"] else 0
        except (ValueError, TypeError):
            continue
    return inv, sold

# --------------------------------------------------------------------------- #
#                            Shopify → nya ordrar                             #
# --------------------------------------------------------------------------- #
def fetch_new_orders(domain: str, token: str, start_date: datetime):
    base = f"https://{domain}/admin/api/2023-07/orders.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    params = {"created_at_min": start_date.isoformat(), "limit": 250, "status": "any"}
    orders = []
    while True:
        r = safe_api_call(requests.get, base, headers=headers, params=params)
        if r.status_code != 200:
            raise RuntimeError(f"Shopify orders error {r.status_code}: {r.text}")
        orders.extend(r.json().get("orders", []))
        link = r.headers.get("Link", "")
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                next_url = part[part.find("<") + 1 : part.find(">")]
                break
        if not next_url:
            break
        base, params = next_url, {}
    print(f"Hämtade totalt {len(orders)} ordrar.")
    return orders

# --------------------------------------------------------------------------- #
#                    Processar ordrar → lager & försäljnings­logg            #
# --------------------------------------------------------------------------- #
def process_orders(
    orders, inventory: Dict[float, int], sold: Dict[float, int], processed: set
):
    new_ids: List[str] = []
    sales_log, sales_log_US = {}, {}

    for o in orders:
        oid = str(o["id"])
        if oid in processed:
            continue
        date_str = datetime.strptime(o["created_at"], "%Y-%m-%dT%H:%M:%S%z").date().isoformat()
        is_US   = (o.get("shipping_address") or {}).get("country_code") == "US"

        for item in o["line_items"]:
            title, qty = item["title"], item["quantity"]
            if "sample" in title.lower():
                continue
            if "Fragrance Bundle" in title:
                for prop in item["properties"]:
                    p = extract_perfume_number(prop["value"])
                    if p is None:
                        continue
                    inventory[p] = inventory.get(p, 0) - qty
                    sold[p]      = sold.get(p, 0)      + qty
                    sales_log.setdefault(date_str, {}).setdefault(p, 0)
                    sales_log[date_str][p] += qty
                    if is_US:
                        sales_log_US.setdefault(date_str, {}).setdefault(p, 0)
                        sales_log_US[date_str][p] += qty
            else:
                p = extract_perfume_number(title)
                if p is None:
                    continue
                inventory[p] = inventory.get(p, 0) - qty
                sold[p]      = sold.get(p, 0)      + qty
                sales_log.setdefault(date_str, {}).setdefault(p, 0)
                sales_log[date_str][p] += qty
                if is_US:
                    sales_log_US.setdefault(date_str, {}).setdefault(p, 0)
                    sales_log_US[date_str][p] += qty
        new_ids.append(oid)
    return new_ids, sales_log, sales_log_US

# --------------------------------------------------------------------------- #
#                       Skriver försäljnings­loggar till Sheets               #
# --------------------------------------------------------------------------- #
def ensure_column(sheet_obj, pnum: float) -> int:
    headers = safe_api_call(sheet_obj.row_values, 1)
    label = fmt_perfume(pnum)
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
    vals = safe_api_call(sheet_obj.get_all_values)
    updates = []
    for d, fdict in sorted(log.items()):
        row = ensure_row(sheet_obj, d, vals)
        for p, q in fdict.items():
            col = ensure_column(sheet_obj, p)
            cur_val = safe_api_call(sheet_obj.cell, row, col).value or "0"
            new_val = int(cur_val) + q
            updates.append(gspread.Cell(row, col, new_val))
    if updates:
        safe_api_call(sheet_obj.update_cells, updates)

# --------------------------------------------------------------------------- #
#                         7-dagars rullande genomsnitt                        #
# --------------------------------------------------------------------------- #
def update_7d_average():
    sales_data = safe_api_call(sales_sheet.get_all_values)
    if not sales_data or len(sales_data) < 2:
        return

    headers = sales_data[0]
    today   = datetime.utcnow().date()
    win_set = {today - timedelta(d) for d in range(7)}

    sums: Dict[float, int] = {}
    for row in sales_data[1:]:  # hoppa header
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
    p_to_row = {}
    for i, r in enumerate(blad1_vals, 1):
        if i == 1 or not r or not r[0]:
            continue
        try:
            p_to_row[float(r[0])] = i
        except ValueError:
            continue

    cells = [
        gspread.Cell(p_to_row[p], 4, round(total / 7, 2))
        for p, total in sums.items()
        if p in p_to_row
    ]
    if cells:
        safe_api_call(sheet.update_cell, 1, 4, "Snitt 7d (per dag)")
        safe_api_call(sheet.update_cells, cells)

# --------------------------------------------------------------------------- #
#                                    MAIN                                     #
# --------------------------------------------------------------------------- #
def main():
    print("=== OBC Lager-script (Endast Store 1) ===")
    init_tables()

    # Engångs-synk vid första körningen
    if not inventory_initialized():
        print("Första körningen – synkar lagersaldo från Shopify …")
        inv = fetch_shopify_inventory(SHOP_DOMAIN, SHOPIFY_TOKEN)
        write_inventory_to_sheet(inv)
        mark_inventory_initialized()
        print("Synk klar ✅\n")

    # 1) Läs lager & Sold
    inventory, sold = read_inventory_and_sold()

    # 2) Ladda redan processade order-ID
    processed = processed_order_ids()

    # 3) Hämta nya ordrar
    START_DATE = datetime(2025, 7, 2)
    print(f"Hämtar ordrar från {START_DATE} …")
    orders = fetch_new_orders(SHOP_DOMAIN, SHOPIFY_TOKEN, START_DATE)

    # 4) Processa
    new_ids, sales_log, sales_log_US = process_orders(orders, inventory, sold, processed)

    # 5) Spara nya ID
    save_processed(new_ids)

    # 6) Logga försäljning
    log_sales(sales_log,    sales_sheet)
    log_sales(sales_log_US, sales_sheet_US)

    # 7) Skriv tillbaka lager & Sold
    blad1_vals = safe_api_call(sheet.get_all_values)
    p_to_row = {float(r[0]): i for i, r in enumerate(blad1_vals, 1)
                if i > 1 and r and r[0]}
    inv_cells, sold_cells = [], []
    for p, q in inventory.items():
        if p in p_to_row:
            inv_cells.append(gspread.Cell(p_to_row[p], 2, q))
    for p, q in sold.items():
        if p in p_to_row:
            sold_cells.append(gspread.Cell(p_to_row[p], 3, q))
    if inv_cells:
        safe_api_call(sheet.update_cells, inv_cells)
    if sold_cells:
        safe_api_call(sheet.update_cells, sold_cells)

    # 8) Uppdatera 7-dagars snitt
    update_7d_average()

    print("✔ Klart", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "UTC")

# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    main()
