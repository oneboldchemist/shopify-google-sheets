import os
import sys
import re
import time
import json
import requests
import gspread
import psycopg2

from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

##############################################################################
#                           Environment Variables                            #
##############################################################################

# === Butik 1 (ingen prefix) ===
SHOP_DOMAIN_1 = os.getenv("SHOP_DOMAIN_1") or "first-shop.myshopify.com"
SHOPIFY_ACCESS_TOKEN_1 = os.getenv("SHOPIFY_ACCESS_TOKEN_1") or "access-token-shop-1"

# === Butik 2 (prefix) ===
SHOP_DOMAIN_2 = os.getenv("SHOP_DOMAIN_2") or "second-shop.myshopify.com"
SHOPIFY_ACCESS_TOKEN_2 = os.getenv("SHOPIFY_ACCESS_TOKEN_2") or "access-token-shop-2"

# Bygg en lista med "konfigurationer" för respektive butik
SHOPIFY_CONFIGS = [
    {
        "domain": SHOP_DOMAIN_1,
        "access_token": SHOPIFY_ACCESS_TOKEN_1,
        "use_prefix": False  # <- Butik 1 får INGEN prefix
    },
    {
        "domain": SHOP_DOMAIN_2,
        "access_token": SHOPIFY_ACCESS_TOKEN_2,
        "use_prefix": True   # <- Butik 2 får prefix
    }
]

# Google credentials (JSON) and scope
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not GOOGLE_CREDS_JSON:
    raise ValueError("Missing environment variable: GOOGLE_CREDENTIALS_JSON")

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds_dict = json.loads(GOOGLE_CREDS_JSON)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# PostgreSQL connection
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("Missing environment variable: DATABASE_URL")

##############################################################################
#                           PostgreSQL Utilities                             #
##############################################################################

def create_table_if_not_exists():
    """
    Du behåller samma tabell som innan, så att redan sparade order-ID utan prefix
    fortfarande är giltiga. Primärnyckeln är 'order_id' (TEXT).
    """
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_orders (
            order_id TEXT PRIMARY KEY,
            processed_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()

def load_processed_orders_from_db():
    """
    Hämtar alla order_id från 'processed_orders' som en set.
    (Dessa kan vara både "123456" och "second-shop.myshopify.com_123456".)
    """
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("SELECT order_id FROM processed_orders;")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return set(row[0] for row in rows)

def save_processed_orders_to_db(order_ids):
    """
    order_ids är en lista med strängar (som ev. har prefix eller inte).
    ON CONFLICT DO NOTHING undviker duplicering.
    """
    if not order_ids:
        return
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO processed_orders (order_id)
        VALUES (%s)
        ON CONFLICT (order_id) DO NOTHING;
    """
    for oid in order_ids:
        cursor.execute(insert_query, (oid,))

    conn.commit()
    cursor.close()
    conn.close()

##############################################################################
#                                 Utilities                                  #
##############################################################################

def safe_api_call(func, *args, **kwargs):
    try:
        result = func(*args, **kwargs)
        time.sleep(2)
        return result
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 429:
            print("Google Sheets API rate limit exceeded, waiting 60 seconds...")
            time.sleep(60)
            return safe_api_call(func, *args, **kwargs)
        else:
            raise e

def format_perfume_number_for_sheet(num_float: float) -> str:
    if num_float.is_integer():
        return str(int(num_float))
    return str(num_float)

def extract_perfume_number(value: str):
    match = re.search(r"(\d{1,3}(?:\.\d+)?)\b", value)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None

##############################################################################
#                         Google Sheets Interactions                         #
##############################################################################

# "Blad1" (huvudlager)
sheet = client.open("OBC lager").sheet1

# "Blad2" (alla ordrar, daglig försäljning)
sales_sheet = client.open("OBC lager").worksheet("Blad2")

# --- NY KOD ---
# "Blad3" (enbart ordrar till USA)
sales_sheet_usa = client.open("OBC lager").worksheet("Blad3")
# --------------

def get_inventory_and_sold():
    print("Fetching inventory and sold amounts from Google Sheets...")
    expected_headers = ['nummer:', 'Antal:', 'Sold:']
    records = safe_api_call(sheet.get_all_records, expected_headers=expected_headers)

    inventory = {}
    sold_data = {}

    for row in records:
        try:
            nummer_value = row['nummer:']
            antal_value = row['Antal:']
            sold_value = row['Sold:']

            if antal_value == '' or antal_value is None:
                continue

            nummer_float = float(nummer_value)

            # Inventory
            if isinstance(antal_value, int):
                inventory[nummer_float] = antal_value
            else:
                inventory[nummer_float] = int(antal_value.replace('−', '-').strip())

            # Sold
            if sold_value == '' or sold_value is None:
                sold_data[nummer_float] = 0
            else:
                if isinstance(sold_value, int):
                    sold_data[nummer_float] = sold_value
                else:
                    sold_data[nummer_float] = int(sold_value.strip())

        except ValueError as e:
            print(f"Warning: Invalid row data: {row}, error: {e}")
            continue

    print("Inventory and sold amounts fetched.")
    return inventory, sold_data

def ensure_columns_for_fragrance(fragrance_sheet, fragrance_number_float):
    """
    Hjälpfunktion för att säkerställa att rätt parfym-kolumn finns i
    antingen Blad2 eller Blad3 (skickas in som 'fragrance_sheet').
    """
    fragrance_header_str = format_perfume_number_for_sheet(fragrance_number_float)
    current_headers = safe_api_call(fragrance_sheet.row_values, 1)
    if fragrance_header_str in current_headers:
        return
    safe_api_call(fragrance_sheet.add_cols, 1)
    new_col_index = len(current_headers) + 1
    safe_api_call(fragrance_sheet.update_cell, 1, new_col_index, fragrance_header_str)
    print(f"Added column for fragrance '{fragrance_header_str}' at col {new_col_index} in sheet '{fragrance_sheet.title}'.")

def find_or_create_row_for_date(fragrance_sheet, date_str, sales_data):
    """
    Söker en existerande rad med date_str i första kolumnen,
    annars infogar en ny rad på rätt datum-sorterad plats.
    """
    all_dates = [row[0] for row in sales_data]
    if date_str in all_dates:
        return all_dates.index(date_str) + 1
    all_dates_sorted = sorted(all_dates[1:] + [date_str])
    insert_pos = all_dates_sorted.index(date_str) + 2
    safe_api_call(fragrance_sheet.insert_row, [date_str], insert_pos)
    print(f"Inserted a new row for date {date_str} at position {insert_pos} in sheet '{fragrance_sheet.title}'.")
    return insert_pos

def update_sales_data(sales_log):
    """
    Uppdaterar Blad2 med ny försäljning från sales_log.
    sales_log: {datum_str: {parfym_float: antal_sold, ...}, ...}
    """
    if not sales_log:
        print("No sales data to update in Blad2.")
        return

    print("Updating sales data in Blad2...")
    sales_data = safe_api_call(sales_sheet.get_all_values)

    headers = sales_data[0] if len(sales_data) > 0 else []
    sorted_sales_log = dict(sorted(sales_log.items()))
    cell_updates = []

    for date_str, fragrance_dict in sorted_sales_log.items():
        row_index = find_or_create_row_for_date(sales_sheet, date_str, sales_data)
        if row_index - 1 >= len(sales_data):
            needed_rows = (row_index - len(sales_data))
            for _ in range(needed_rows):
                blank_row = [''] * max(1, len(headers))
                sales_data.append(blank_row)

        for fragrance_number_float, qty_sold in fragrance_dict.items():
            ensure_columns_for_fragrance(sales_sheet, fragrance_number_float)
            headers = safe_api_call(sales_sheet.row_values, 1)

            fragrance_header_str = format_perfume_number_for_sheet(fragrance_number_float)
            try:
                col_index = headers.index(fragrance_header_str) + 1
            except ValueError:
                print(f"Could not find header '{fragrance_header_str}' after ensuring columns.")
                continue

            try:
                cell_obj = safe_api_call(sales_sheet.cell, row_index, col_index)
                current_value_str = cell_obj.value if cell_obj else ''
            except Exception as e:
                print(f"Warning: failed to read cell R{row_index}C{col_index} => {e}")
                current_value_str = ''

            current_value = int(current_value_str) if current_value_str else 0
            new_value = current_value + qty_sold
            cell_updates.append((row_index, col_index, new_value))

    if cell_updates:
        print(f"Performing batch update of {len(cell_updates)} sales cells (Blad2)...")
        gspread_cells = [gspread.Cell(r, c, val) for (r, c, val) in cell_updates]
        safe_api_call(sales_sheet.update_cells, gspread_cells)
        print("Sales data updated in Blad2.")
    else:
        print("No cells to update in sales data for Blad2.")

# --- NY KOD ---
def update_sales_data_usa(sales_log_usa):
    """
    Samma logik som update_sales_data, men för Blad3.
    sales_log_usa: {datum_str: {parfym_float: antal_sold, ...}, ...}
    """
    if not sales_log_usa:
        print("No sales data to update in Blad3.")
        return

    print("Updating sales data in Blad3 (USA only)...")
    sales_data = safe_api_call(sales_sheet_usa.get_all_values)

    headers = sales_data[0] if len(sales_data) > 0 else []
    sorted_sales_log = dict(sorted(sales_log_usa.items()))
    cell_updates = []

    for date_str, fragrance_dict in sorted_sales_log.items():
        row_index = find_or_create_row_for_date(sales_sheet_usa, date_str, sales_data)
        if row_index - 1 >= len(sales_data):
            needed_rows = (row_index - len(sales_data))
            for _ in range(needed_rows):
                blank_row = [''] * max(1, len(headers))
                sales_data.append(blank_row)

        for fragrance_number_float, qty_sold in fragrance_dict.items():
            ensure_columns_for_fragrance(sales_sheet_usa, fragrance_number_float)
            headers = safe_api_call(sales_sheet_usa.row_values, 1)

            fragrance_header_str = format_perfume_number_for_sheet(fragrance_number_float)
            try:
                col_index = headers.index(fragrance_header_str) + 1
            except ValueError:
                print(f"Could not find header '{fragrance_header_str}' after ensuring columns in Blad3.")
                continue

            try:
                cell_obj = safe_api_call(sales_sheet_usa.cell, row_index, col_index)
                current_value_str = cell_obj.value if cell_obj else ''
            except Exception as e:
                print(f"Warning: failed to read cell R{row_index}C{col_index} in Blad3 => {e}")
                current_value_str = ''

            current_value = int(current_value_str) if current_value_str else 0
            new_value = current_value + qty_sold
            cell_updates.append((row_index, col_index, new_value))

    if cell_updates:
        print(f"Performing batch update of {len(cell_updates)} sales cells (Blad3)...")
        gspread_cells = [gspread.Cell(r, c, val) for (r, c, val) in cell_updates]
        safe_api_call(sales_sheet_usa.update_cells, gspread_cells)
        print("Sales data updated in Blad3.")
    else:
        print("No cells to update in sales data for Blad3.")
# --------------

##############################################################################
#                  Ny funktion för rullande 7-dagars snitt                  #
##############################################################################

def update_7d_rolling_average_in_blad1():
    """
    Beräknar och uppdaterar en extra kolumn i Blad1 (kolumn D) med rubriken "Snitt 7d (per dag)",
    som visar snittförsäljning per dag över de senaste 7 kalenderdagarna.
    """
    # Hämta all data från Blad2 (försäljningsloggen)
    sales_data = safe_api_call(sales_sheet.get_all_values)
    if not sales_data:
        print("Blad2 saknar data. Hoppar över 7-dagars uppdatering.")
        return
    
    headers = sales_data[0]  # Första raden i Blad2 (parfymnummer som rubriker)
    if len(headers) < 2:
        print("Blad2 saknar giltiga kolumnrubriker. Hoppar över 7-dagars uppdatering.")
        return

    # Förbered en struktur för försäljning per parfym, per datum
    # sales_map[ parfym_float ] = { date_obj: antal_sålda, ... }
    sales_map = {}
    
    # Datumintervall: senaste 7 kalenderdagarna, inkl idag
    today = datetime.now().date()
    seven_days_ago = today - timedelta(days=6)
    date_format = "%Y-%m-%d"

    # Hitta parfym-kolumner (dvs headers för parfym-nummer)
    perfume_columns = []
    for col_index in range(1, len(headers)):
        try:
            perfume_number = float(headers[col_index])  # t.ex. "35" -> 35.0
            perfume_columns.append((col_index, perfume_number))
        except ValueError:
            continue  # hoppa kolumner som ej är parfymnummer

    # Loopa igenom varje rad i Blad2 (förutom headern)
    for row in sales_data[1:]:
        if not row or len(row) == 0:
            continue
        
        date_str = row[0].strip()
        try:
            row_date = datetime.strptime(date_str, date_format).date()
        except ValueError:
            # Ogiltigt datumformat i kolumn A
            continue
        
        if row_date < seven_days_ago or row_date > today:
            continue  # ligger inte i 7-dagarsfönstret
        
        for (c_idx, p_number) in perfume_columns:
            if c_idx >= len(row):
                continue
            cell_value = row[c_idx].strip() if c_idx < len(row) else "0"
            try:
                qty_sold = int(cell_value) if cell_value else 0
            except ValueError:
                qty_sold = 0

            if p_number not in sales_map:
                sales_map[p_number] = {}
            sales_map[p_number][row_date] = sales_map[p_number].get(row_date, 0) + qty_sold

    # Räkna ut snitt per parfym (summa / 7)
    perfume_7d_avg = {}
    for p_number, day_dict in sales_map.items():
        total_7d = 0
        for d_offset in range(7):  # 0..6
            check_date = seven_days_ago + timedelta(days=d_offset)
            total_7d += day_dict.get(check_date, 0)
        perfume_7d_avg[p_number] = round(total_7d / 7.0, 2)

    # Hämta data från Blad1 för att uppdatera i kolumn D
    blad1_data = safe_api_call(sheet.get_all_values)
    if not blad1_data:
        print("Blad1 saknar data. Hoppar över 7-dagars uppdatering.")
        return

    # Kontrollera att vi minst har 4 kolumner (A, B, C, D)
    # och att D1 = "Snitt 7d (per dag)".
    # Om kolumn D inte finns ska vi skapa den.
    header_row = blad1_data[0] if blad1_data else []
    num_cols = len(header_row)
    desired_header = "Snitt 7d (per dag)"

    if num_cols < 4:
        # Lägg till kolumner tills vi har minst 4
        cols_to_add = 4 - num_cols
        safe_api_call(sheet.add_cols, cols_to_add)
        # Efter detta är num_cols >= 4
        num_cols = 4
        # Hämta om headern på nytt
        header_row = safe_api_call(sheet.row_values, 1)

    # Sätt text i D1 om den inte redan är "Snitt 7d (per dag)"
    if len(header_row) < 4 or header_row[3] != desired_header:
        safe_api_call(sheet.update_cell, 1, 4, desired_header)
        print(f"Skrev '{desired_header}' i D1.")

    # Bygg mappning parfym_nr -> radindex i Blad1, utgår ifrån kolumn A = "nummer:"
    perfume_to_row = {}
    for i, row_data in enumerate(blad1_data, start=1):
        if i == 1:
            continue  # header-raden
        if len(row_data) < 1 or not row_data[0].strip():
            continue
        try:
            p_float = float(row_data[0].strip())  # A-kolumnen tolkas som parfymnr
            perfume_to_row[p_float] = i
        except ValueError:
            pass

    # Uppdatera celler i kolumn D med våra snittvärden
    cell_updates = []
    for p_number, avg_value in perfume_7d_avg.items():
        row_idx = perfume_to_row.get(p_number)
        if not row_idx:
            # Parfymen finns inte i Blad1
            continue
        # Kolumn D = index 4
        cell_updates.append(gspread.Cell(row_idx, 4, avg_value))

    if cell_updates:
        safe_api_call(sheet.update_cells, cell_updates)
        print(f"Uppdaterade rullande 7-dagars snitt för {len(cell_updates)} parfymer i kolumn D.")
    else:
        print("Inga 7-dagarsvärden att uppdatera i kolumn D.")

##############################################################################
#                           Shopify Order Handling                           #
##############################################################################

def fetch_new_orders(shop_domain, shopify_access_token, start_date):
    base_url = f"https://{shop_domain}/admin/api/2023-07"
    endpoint = f"{base_url}/orders.json"
    headers = {
        "X-Shopify-Access-Token": shopify_access_token,
        "Content-Type": "application/json"
    }
    params = {
        "created_at_min": start_date.isoformat(),
        "limit": 250,
        "status": "any"
    }

    print(f"Fetching new orders from {shop_domain} since {start_date}...")
    orders = []

    while True:
        response = safe_api_call(requests.get, endpoint, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            fetched_orders = data["orders"]
            orders.extend(fetched_orders)
            print(f"Fetched {len(fetched_orders)} orders from {shop_domain}.")

            link_header = response.headers.get('Link')
            if link_header:
                next_link = None
                links = link_header.split(',')
                for link_part in links:
                    if 'rel="next"' in link_part:
                        next_link = link_part[link_part.find("<")+1:link_part.find(">")]
                        break
                if next_link:
                    endpoint = next_link
                    params = {}
                    print("Fetching next page of orders...")
                    continue
            break
        else:
            print(f"Failed to fetch orders from {shop_domain}. Status: {response.status_code}, Message: {response.text}")
            break

    print(f"Total orders fetched from {shop_domain}: {len(orders)}")
    return orders

def process_orders(shop_domain, orders, inventory, sold, already_processed_orders, use_prefix):
    """
    Processar ordrar för en specifik butik (shop_domain).
    'use_prefix' avgör om vi ska prefixa order-ID eller inte.
    Returnerar (new_processed_order_ids, sales_log, sales_log_usa).
    """
    print(f"Processing orders for {shop_domain}...")
    new_processed_order_ids = []
    sales_log = {}

    # --- NY KOD ---
    # Separat försäljningslogg för ordrar med shipping_address i USA
    sales_log_usa = {}
    # --------------

    for order in orders:
        raw_id = str(order['id'])

        # === Skapa unikt order-ID beroende på prefix ===
        if use_prefix:
            unique_order_id = f"{shop_domain}_{raw_id}"
        else:
            unique_order_id = raw_id  # Butik 1 använder "klassisk" lagring

        if unique_order_id in already_processed_orders:
            continue  # redan processad

        print(f"\nProcessing Order ID: {unique_order_id}")
        order_date_str = datetime.strptime(
            order['created_at'], "%Y-%m-%dT%H:%M:%S%z"
        ).date().strftime("%Y-%m-%d")

        # Kolla om ordern är från USA
        is_usa_order = False
        if order.get("shipping_address") and order["shipping_address"].get("country_code") == "US":
            is_usa_order = True

        for item in order['line_items']:
            title = item['title']
            quantity = item['quantity']

            # Hoppa över "sample"
            if "sample" in title.lower():
                continue

            # Bundle?
            if "Fragrance Bundle" in title:
                print(f"Processing bundle: {title}")
                perfumes_processed = []
                for prop in item['properties']:
                    perfume_number = extract_perfume_number(prop['value'])
                    if perfume_number is not None and perfume_number in inventory:
                        # Minska lager
                        inventory[perfume_number] -= quantity
                        sold[perfume_number] += quantity
                        # Logga i "vanliga" sales_log (Blad2)
                        sales_log.setdefault(order_date_str, {}).setdefault(perfume_number, 0)
                        sales_log[order_date_str][perfume_number] += quantity

                        # --- NY KOD ---
                        if is_usa_order:
                            # Logga även i sales_log_usa (Blad3)
                            sales_log_usa.setdefault(order_date_str, {}).setdefault(perfume_number, 0)
                            sales_log_usa[order_date_str][perfume_number] += quantity
                        # -------------

                        perfumes_processed.append(perfume_number)
                        print(f"Perfume {perfume_number} => new inventory: {inventory[perfume_number]}")
                    else:
                        print(f"Perfume number '{prop['value']}' not found in inventory.")

                # Kolla om det var 3 eller 2 parfymer i bundle
                expected_count = 3 if "3x" in title else 2
                if len(perfumes_processed) != expected_count:
                    print(f"Warning: Expected {expected_count} in bundle, found {len(perfumes_processed)}.")
            else:
                # Vanlig produkt
                perfume_number = extract_perfume_number(title)
                if perfume_number is not None and perfume_number in inventory:
                    inventory[perfume_number] -= quantity
                    sold[perfume_number] += quantity

                    # Logga i "vanliga" sales_log (Blad2)
                    sales_log.setdefault(order_date_str, {}).setdefault(perfume_number, 0)
                    sales_log[order_date_str][perfume_number] += quantity

                    # --- NY KOD ---
                    if is_usa_order:
                        # Logga även i sales_log_usa (Blad3)
                        sales_log_usa.setdefault(order_date_str, {}).setdefault(perfume_number, 0)
                        sales_log_usa[order_date_str][perfume_number] += quantity
                    # -------------

                    print(f"Perfume {perfume_number} => new inventory: {inventory[perfume_number]}")
                else:
                    print(f"Perfume number for '{title}' not found in inventory.")

        # Markera ordern som processad
        new_processed_order_ids.append(unique_order_id)

    # Returnerar två separata försäljningsloggar
    return new_processed_order_ids, sales_log, sales_log_usa

##############################################################################
#                                   MAIN                                     #
##############################################################################

def main():
    try:
        print("Starting main process...")

        # 1. Säkerställ att tabellen finns
        create_table_if_not_exists()

        # 2. Exempel: bearbeta ordrar från 7 januari 2025 kl 13:02
        start_date = datetime(2025, 1, 7, 13, 2)

        # 3. Ladda redan processade order IDs (utan och med prefix)
        processed_orders = load_processed_orders_from_db()

        # 4. Hämta inventory och sold från Google Sheets (delas av båda butikerna)
        inventory, sold_data = get_inventory_and_sold()

        # Samla upp alla nya processade ID:s
        all_new_processed_ids = []

        # Samlar även upp all försäljning i en gemensam dictionary (vanlig)
        total_sales_log = {}
        # --- NY KOD ---
        # Samlar upp all försäljning i en gemensam dictionary (bara USA)
        total_sales_log_usa = {}
        # --------------

        for shop_cfg in SHOPIFY_CONFIGS:
            shop_domain = shop_cfg["domain"]
            shopify_access_token = shop_cfg["access_token"]
            use_prefix = shop_cfg["use_prefix"]

            # 5. Hämta nya ordrar för denna butik
            orders = fetch_new_orders(shop_domain, shopify_access_token, start_date)
            if not orders:
                print(f"No new orders found for {shop_domain}.")
                continue

            # 6. Processa ordrar
            new_ids, sales_log, sales_log_usa = process_orders(
                shop_domain, orders, inventory, sold_data,
                processed_orders, use_prefix
            )
            all_new_processed_ids.extend(new_ids)

            # Slå ihop shopspecifik försäljning med totalen
            # (nyckel = datum, value = {parfymnr: qty})
            for date_str, fdict in sales_log.items():
                total_sales_log.setdefault(date_str, {})
                for pnr, qty in fdict.items():
                    total_sales_log[date_str].setdefault(pnr, 0)
                    total_sales_log[date_str][pnr] += qty

            # --- NY KOD: USA-sammanställning ---
            for date_str, fdict in sales_log_usa.items():
                total_sales_log_usa.setdefault(date_str, {})
                for pnr, qty in fdict.items():
                    total_sales_log_usa[date_str].setdefault(pnr, 0)
                    total_sales_log_usa[date_str][pnr] += qty
            # -----------------------------------

        # 7a. Spara nydligen processade order-ID i DB
        save_processed_orders_to_db(all_new_processed_ids)

        # 7b. Uppdatera Google Sheets "Blad2" (daglig försäljning, alla ordrar)
        if total_sales_log:
            update_sales_data(total_sales_log)
        else:
            print("No overall sales data to log in Blad2.")

        # --- NY KOD ---
        # 7c. Uppdatera Google Sheets "Blad3" (daglig försäljning, endast USA)
        if total_sales_log_usa:
            update_sales_data_usa(total_sales_log_usa)
        else:
            print("No USA sales data to log in Blad3.")
        # --------------

        # 8. Uppdatera lager (inventory + sold) i Blad1
        print("Preparing to batch update inventory and sold in the main sheet...")
        sheet_values = safe_api_call(sheet.get_all_values)

        # Mappa parfym-float => radindex
        perfume_to_row = {}
        for row_i, row_data in enumerate(sheet_values, start=1):
            if row_i == 1:
                continue
            if not row_data or len(row_data) < 1:
                continue
            try:
                sheet_perfume_str = row_data[0].strip()
                sheet_perfume_float = float(sheet_perfume_str)
                perfume_to_row[sheet_perfume_float] = row_i
            except ValueError:
                continue

        inventory_updates = []
        sold_updates = []

        for perfume_float, new_antal in inventory.items():
            row_index = perfume_to_row.get(perfume_float)
            if not row_index:
                print(f"Perfume {perfume_float} not found in main sheet. Skipping inventory update.")
                continue
            inventory_updates.append(gspread.Cell(row_index, 2, new_antal))

        for perfume_float, new_sold in sold_data.items():
            row_index = perfume_to_row.get(perfume_float)
            if not row_index:
                print(f"Perfume {perfume_float} not found in main sheet. Skipping sold update.")
                continue
            sold_updates.append(gspread.Cell(row_index, 3, new_sold))

        if inventory_updates:
            print(f"Batch updating {len(inventory_updates)} inventory cells...")
            safe_api_call(sheet.update_cells, inventory_updates)

        if sold_updates:
            print(f"Batch updating {len(sold_updates)} sold cells...")
            safe_api_call(sheet.update_cells, sold_updates)

        print("Inventory, sold amounts, and daily sales data updated for both shops.")

        # 9. Uppdatera kolumnen D i Blad1 med 7-dagars snitt
        update_7d_rolling_average_in_blad1()

    except Exception as e:
        print(f"Error in main process: {e}")
        print("No changes saved.")
    finally:
        print("Script ended:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":
    print("Starting script...")
    main()
