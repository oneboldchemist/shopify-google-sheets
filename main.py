#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OBC – Lager & Försäljning (ENDAST Store 1)

• Hämtar ENBART ordrar från butik 1.
• Första körningen synkar lagersaldot i Google‑arket med Shopify‑lagret.
• Därefter fungerar skriptet som tidigare (försäljning, 7‑dagars‑snitt, USA‑logg m.m.).
• start_date = 2 juli 2025 (UTC) – ändra vid behov.

Kräver miljövariabler:
  SHOP_DOMAIN_1, SHOPIFY_ACCESS_TOKEN_1,
  GOOGLE_CREDENTIALS_JSON, DATABASE_URL
"""
# --------------------------------------------------------------------------- #
import os, re, time, json, requests, gspread, psycopg2
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
# --------------------------------------------------------------------------- #
#                            Miljö­variabler & API‑setup                     #
# --------------------------------------------------------------------------- #
SHOP_DOMAIN = os.getenv("SHOP_DOMAIN_1") or "first-shop.myshopify.com"
SHOPIFY_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN_1") or "access-token-shop-1"

GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")  # service‑konto‑JSON
if not GOOGLE_CREDS_JSON:
    raise ValueError("Missing env GOOGLE_CREDENTIALS_JSON")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("Missing env DATABASE_URL")

scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(
    json.loads(GOOGLE_CREDS_JSON), scope)
client = gspread.authorize(creds)

# Google Sheets
sheet          = client.open("OBC lager").sheet1           # Blad1
sales_sheet    = client.open("OBC lager").worksheet("Blad2")
sales_sheet_US = client.open("OBC lager").worksheet("Blad3")

# --------------------------------------------------------------------------- #
#                              PostgreSQL‑hjälp­funktioner                    #
# --------------------------------------------------------------------------- #
def pg_conn():  # kort hjälpfunktion
    return psycopg2.connect(DATABASE_URL)

def init_tables():
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_orders (
                order_id TEXT PRIMARY KEY,
                processed_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS inventory_initialized (
                done BOOLEAN PRIMARY KEY  -- finns exakt EN rad när lagret initierats
            );
        """)

def processed_order_ids() -> set[str]:
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT order_id FROM processed_orders;")
        return {row[0] for row in cur.fetchall()}

def save_processed(ids: list[str]):
    if not ids:
        return
    with pg_conn() as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO processed_orders (order_id) VALUES (%s) ON CONFLICT DO NOTHING;",
            [(oid,) for oid in ids]
        )
        conn.commit()

def inventory_already_initialized() -> bool:
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT done FROM inventory_initialized WHERE done = TRUE;")
        return cur.fetchone() is not None

def mark_inventory_initialized():
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO inventory_initialized (done) VALUES (TRUE) ON CONFLICT DO NOTHING;")
        conn.commit()

# --------------------------------------------------------------------------- #
#                               Övriga hjälpare                               #
# --------------------------------------------------------------------------- #
def safe_api_call(func, *args, **kwargs):
    """Rate‑limit friendly wrapper kring Google / Requests."""
    try:
        res = func(*args, **kwargs)
        time.sleep(2)
        return res
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 429:
            print("[Google] 429 – väntar 60 s…")
            time.sleep(60)
            return safe_api_call(func, *args, **kwargs)
        raise

def extract_perfume_number(text: str):
    m = re.search(r"(\d{1,3}(?:\.\d+)?)\b", text)
    return float(m.group(1)) if m else None

def fmt_perfume(num: float) -> str:
    return str(int(num)) if num.is_integer() else str(num)

# --------------------------------------------------------------------------- #
#                  1) Engångs‑synk: hämta Shopify‑lager → Blad1               #
# --------------------------------------------------------------------------- #
def fetch_shopify_inventory(domain: str, token: str) -> dict[float, int]:
    """Läser alla product‑varianter och summerar inventory_quantity per parfymnr."""
    base = f"https://{domain}/admin/api/2023-07/variants.json"
    params = {"limit": 250, "fields": "id,title,sku,inventory_quantity"}
    headers = {"X-Shopify-Access-Token": token}

    inventory = {}
    while True:
        r = safe_api_call(requests.get, base, headers=headers, params=params)
        if r.status_code != 200:
            raise RuntimeError(f"Shopify variants error {r.status_code}: {r.text}")
        data = r.json().get("variants", [])
        for v in data:
            title_or_sku = v.get("sku") or v.get("title", "")
            pnr = extract_perfume_number(title_or_sku)
            if pnr is None:
                continue
            inventory[pnr] = inventory.get(pnr, 0) + int(v["inventory_quantity"])
        # pagination
        link = r.headers.get("Link", "")
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                next_url = part[part.find("<")+1:part.find(">")]
                break
        if not next_url:
            break
        base, params = next_url, {}  # fortsätt via absolute URL
    print(f"[Init‑lager] Hämtade {len(inventory)} parfymnummer från Shopify.")
    return inventory

def write_inventory_to_sheet(inv: dict[float, int]):
    values = safe_api_call(sheet.get_all_values)
    header = values[0] if values else []
    perfume_to_row = {}
    for i, row in enumerate(values, start=1):
        if i == 1 or not row or not row[0].strip():
            continue
        try:
            perfume_to_row[float(row[0])] = i
        except ValueError:
            pass

    new_rows = []
    updates = []
    for pnum, qty in inv.items():
        if pnum in perfume_to_row:
            updates.append(gspread.Cell(perfume_to_row[pnum], 2, qty))  # kol B = Antal:
        else:
            new_rows.append([fmt_perfume(pnum), qty, 0])

    if new_rows:
        safe_api_call(sheet.append_rows, new_rows, value_input_option="USER_ENTERED")
        print(f"[Init‑lager] Lagt till {len(new_rows)} nya rader i Blad1.")
    if updates:
        safe_api_call(sheet.update_cells, updates)
        print(f"[Init‑lager] Uppdaterat lagersaldo för {len(updates)} rader i Blad1.")

# --------------------------------------------------------------------------- #
#                         2) Inläsning av aktuellt lager                      #
# --------------------------------------------------------------------------- #
def get_inventory_and_sold():
    print("Läser lager & Sold från Blad1 …")
    recs = safe_api_call(sheet.get_all_records,
                         expected_headers=['nummer:', 'Antal:', 'Sold:'])
    inv, sold = {}, {}
    for row in recs:
        try:
            num = float(row['nummer:'])
            inv[num]   = int(str(row['Antal:']).replace('−', '-').strip()) if row['Antal:'] != '' else 0
            sold[num]  = int(str(row['Sold:']).strip())                    if row['Sold:']  != '' else 0
        except (ValueError, TypeError):
            continue
    return inv, sold

# --------------------------------------------------------------------------- #
#                    3) Shopify‑ordrar → försäljning & lager                  #
# --------------------------------------------------------------------------- #
def fetch_new_orders(domain, token, start_date):
    base = f"https://{domain}/admin/api/2023-07/orders.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    params  = {"created_at_min": start_date.isoformat(), "limit": 250, "status": "any"}
    orders  = []
    print(f"Hämtar ordrar från {start_date} …")
    while True:
        r = safe_api_call(requests.get, base, headers=headers, params=params)
        if r.status_code != 200:
            raise RuntimeError(f"Shopify orders error {r.status_code}: {r.text}")
        part = r.json().get("orders", [])
        orders.extend(part)
        link = r.headers.get("Link", "")
        next_url = None
        for prt in link.split(","):
            if 'rel="next"' in prt:
                next_url = prt[prt.find("<")+1:prt.find(">")]
                break
        if not next_url:
            break
        base, params = next_url, {}
    print(f"Hämtade totalt {len(orders)} ordrar.")
    return orders

def process_orders(orders, inventory, sold, already_processed):
    processed_ids, sales_log, sales_log_US = [], {}, {}
    for order in orders:
        oid = str(order['id'])
        if oid in already_processed:
            continue
        date_str = datetime.strptime(order['created_at'], "%Y-%m-%dT%H:%M:%S%z").date().isoformat()
        is_US   = (order.get("shipping_address") or {}).get("country_code") == "US"

        for item in order['line_items']:
            title, qty = item['title'], item['quantity']
            if "sample" in title.lower():
                continue
            if "Fragrance Bundle" in title:
                # antalet ingående parfymer hittas i item['properties']
                for prop in item['properties']:
                    pnum = extract_perfume_number(prop['value'])
                    if pnum is None:  continue
                    inventory[pnum] = inventory.get(pnum, 0) - qty
                    sold[pnum]      = sold.get(pnum, 0)      + qty
                    sales_log.setdefault(date_str, {}).setdefault(pnum, 0)
                    sales_log[date_str][pnum] += qty
                    if is_US:
                        sales_log_US.setdefault(date_str, {}).setdefault(pnum, 0)
                        sales_log_US[date_str][pnum] += qty
            else:
                pnum = extract_perfume_number(title)
                if pnum is None:  continue
                inventory[pnum] = inventory.get(pnum, 0) - qty
                sold[pnum]      = sold.get(pnum, 0)      + qty
                sales_log.setdefault(date_str, {}).setdefault(pnum, 0)
                sales_log[date_str][pnum] += qty
                if is_US:
                    sales_log_US.setdefault(date_str, {}).setdefault(pnum, 0)
                    sales_log_US[date_str][pnum] += qty
        processed_ids.append(oid)
    return processed_ids, sales_log, sales_log_US

# --------------------------------------------------------------------------- #
#           4) Google‑ark‑uppdateringar (samma logik som tidigare)            #
# --------------------------------------------------------------------------- #
def ensure_column(sheet_obj, pnum):
    header = safe_api_call(sheet_obj.row_values, 1)
    h = fmt_perfume(pnum)
    if h in header:
        return header.index(h) + 1
    safe_api_call(sheet_obj.add_cols, 1)
    col = len(header) + 1
    safe_api_call(sheet_obj.update_cell, 1, col, h)
    return col

def ensure_row(sheet_obj, date_str, sheet_values):
    dates = [row[0] for row in sheet_values]
    if date_str in dates:
        return dates.index(date_str) + 1
    ins_pos = sorted(dates[1:] + [date_str]).index(date_str) + 2
    safe_api_call(sheet_obj.insert_row, [date_str], ins_pos)
    return ins_pos

def log_sales(sales_log: dict, sheet_obj):
    if not sales_log:
        return
    data = safe_api_call(sheet_obj.get_all_values)
    cell_updates = []
    for date_str, fdict in sorted(sales_log.items()):
        row = ensure_row(sheet_obj, date_str, data)
        for pnum, qty in fdict.items():
            col = ensure_column(sheet_obj, pnum)
            curr_val = safe_api_call(sheet_obj.cell, row, col).value or "0"
            new_val  = int(curr_val) + qty
            cell_updates.append(gspread.Cell(row, col, new_val))
    if cell_updates:
        safe_api_call(sheet_obj.update_cells, cell_updates)

# --------------------------------------------------------------------------- #
#                 5) 7‑dagars rullande snitt (oförändrad logik)              #
# --------------------------------------------------------------------------- #
def update_7d_average():
    # (samma kod som tidigare men kortare för tydlighet)
    data = safe_api_call(sales_sheet.get_all_values)
    if not data or len(data) < 2:
        return
    headers = data[0]
    today = datetime.utcnow().date()
    window = {today - timedelta(d): d for d in range(7)}
    sums = {}
    for row in data[1:]:
        try:
            d = datetime.strptime(row[0], "%Y-%m-%d").date()
        except (ValueError, IndexError):
            continue
        if d not in window:
            continue
        for c, h in enumerate(headers[1:], 1):
            try:
                pnum = float(h)
                qty  = int(row[c] or 0)
                sums.setdefault(pnum, 0)
                sums[pnum] += qty
            except ValueError:
                continue
    cells = []
    for pnum, total in sums.items():
        try:
            row_idx = next(i for i, r in enumerate(sheet.get_all_values(), 1)
                           if r and r[0] and float(r[0]) == pnum)
        except StopIteration:
            continue
        cells.append(gspread.Cell(row_idx, 4, round(total/7, 2)))
    if cells:
        safe_api_call(sheet.update_cell, 1, 4, "Snitt 7d (per dag)")
        safe_api_call(sheet.update_cells, cells)

# --------------------------------------------------------------------------- #
#                                    MAIN                                     #
# --------------------------------------------------------------------------- #
def main():
    print("=== OBC lager‑script (endast Store 1) ===")
    init_tables()

    # 0) Engångs‑initialisering av lagersaldo
    if not inventory_already_initialized():
        print("Första körningen – synkar lagersaldon från Shopify …")
        shopify_inv = fetch_shopify_inventory(SHOP_DOMAIN, SHOPIFY_TOKEN)
        write_inventory_to_sheet(shopify_inv)
        mark_inventory_initialized()
        print("Synk klar ✅\n")

    # 1) Läs lager & sold från Blad1
    inventory, sold = get_inventory_and_sold()

    # 2) Hämta redan processade orders
    processed = processed_order_ids()

    # 3) Hämta nya ordrar
    START_DATE = datetime(2025, 7, 2)   # <-- enligt instruktion
    orders = fetch_new_orders(SHOP_DOMAIN, SHOPIFY_TOKEN, START_DATE)

    # 4) Processa
    new_ids, sales_log, sales_log_US = process_orders(
        orders, inventory, sold, processed)

    # 5) Spara order‑ID
    save_processed(new_ids)

    # 6) Logga försäljning
    log_sales(sales_log,    sales_sheet)
    log_sales(sales_log_US, sales_sheet_US)

    # 7) Skriv tillbaka lager & sold (kol B + C)
    sheet_vals = safe_api_call(sheet.get_all_values)
    perfume_to_row = {float(r[0]): i for i, r in enumerate(sheet_vals, 1)
                      if i > 1 and r and r[0]}
    inv_cells, sold_cells = [], []
    for pnum, qty in inventory.items():
        row = perfume_to_row.get(pnum)
        if row:
            inv_cells .append(gspread.Cell(row, 2, qty))
    for pnum, qty in sold.items():
        row = perfume_to_row.get(pnum)
        if row:
            sold_cells.append(gspread.Cell(row, 3, qty))
    if inv_cells:
        safe_api_call(sheet.update_cells, inv_cells)
    if sold_cells:
        safe_api_call(sheet.update_cells, sold_cells)

    # 8) Uppdatera 7‑dagars snitt
    update_7d_average()

    print("Klar – ", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "UTC")

# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    main()
