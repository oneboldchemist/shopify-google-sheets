import os
import sys
import re
import time
import json
import requests
import gspread
import psycopg2

from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

##############################################################################
#                           Environment Variables                            #
##############################################################################

# Shopify credentials
SHOP_DOMAIN = os.getenv("SHOP_DOMAIN") or "your-shop-domain.myshopify.com"
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN") or "your-access-token"
BASE_URL = f"https://{SHOP_DOMAIN}/admin/api/2023-07"

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
    Create a 'processed_orders' table if it does not exist.
    Columns:
      - order_id (TEXT, primary key)
      - processed_at (TIMESTAMP, defaults to NOW())
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
    Fetch all order IDs from the 'processed_orders' table and return them as a set.
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
    Insert new order IDs into 'processed_orders' table.
    Uses ON CONFLICT DO NOTHING to avoid duplicates if an ID already exists.
    """
    if not order_ids:
        return  # No new orders to save
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO processed_orders (order_id)
        VALUES (%s)
        ON CONFLICT (order_id) DO NOTHING;
    """
    for order_id in order_ids:
        cursor.execute(insert_query, (order_id,))

    conn.commit()
    cursor.close()
    conn.close()

##############################################################################
#                                 Utilities                                  #
##############################################################################

def safe_api_call(func, *args, **kwargs):
    """
    Wrapper for calls to the Google Sheets or Shopify API.
    - Sleeps 2 seconds after each call (to avoid rate limits).
    - If a 429 status code is encountered (rate limit), waits 60 seconds and retries.
    """
    try:
        result = func(*args, **kwargs)
        time.sleep(2)
        return result
    except gspread.exceptions.APIError as e:
        # If Google Sheets API rate limit
        if e.response.status_code == 429:
            print("Google Sheets API rate limit exceeded, waiting 60 seconds...")
            time.sleep(60)
            return safe_api_call(func, *args, **kwargs)
        else:
            raise e

def format_perfume_number_for_sheet(num_float: float) -> str:
    """
    Convert a float perfume number (e.g. 149.0) into a sheet-friendly string.
    - If integer float (149.0), convert to '149'
    - Otherwise keep e.g. '149.5'
    """
    if num_float.is_integer():
        return str(int(num_float))
    return str(num_float)

def extract_perfume_number(value: str):
    """
    Regex to capture up to 3 digits and optional decimal part (e.g. 22.0, 149, 149.0).
    Returns a float or None if no match found.
    """
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

# Open the Google Sheets (must have a sheet named "OBC lager" with these worksheets)
sheet = client.open("OBC lager").sheet1              # Main sheet with inventory
sales_sheet = client.open("OBC lager").worksheet("Blad2")  # "Blad2" for daily sales

def get_inventory_and_sold():
    """
    Fetch inventory (Antal) and sold amounts (Sold) from the main sheet.
    Returns two dicts keyed by float perfume number:
      - inventory[perfume_number_float] = int
      - sold[perfume_number_float]      = int
    """
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

            # Convert inventory to int
            if isinstance(antal_value, int):
                inventory[nummer_float] = antal_value
            else:
                inventory[nummer_float] = int(antal_value.replace('−', '-').strip())

            # Convert sold to int
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

def ensure_columns_for_fragrance(fragrance_number_float):
    """
    Ensure 'Blad2' has a column header for this fragrance.
    """
    fragrance_header_str = format_perfume_number_for_sheet(fragrance_number_float)
    current_headers = safe_api_call(sales_sheet.row_values, 1)
    if fragrance_header_str in current_headers:
        return  # Already present

    safe_api_call(sales_sheet.add_cols, 1)
    new_col_index = len(current_headers) + 1
    safe_api_call(sales_sheet.update_cell, 1, new_col_index, fragrance_header_str)
    print(f"Added column for fragrance '{fragrance_header_str}' at col {new_col_index}.")

def find_or_create_row_for_date(date_str, sales_data):
    """
    Look for 'date_str' in the first column of 'Blad2'.
    If not found, insert a new row in sorted position.
    Return the 1-based row index in the sheet.
    """
    all_dates = [row[0] for row in sales_data]
    if date_str in all_dates:
        return all_dates.index(date_str) + 1

    all_dates_sorted = sorted(all_dates[1:] + [date_str])
    insert_pos = all_dates_sorted.index(date_str) + 2
    safe_api_call(sales_sheet.insert_row, [date_str], insert_pos)
    print(f"Inserted a new row for date {date_str} at position {insert_pos}")
    return insert_pos

def update_sales_data(sales_log):
    """
    Update 'Blad2' (daily sales) in a batched manner.
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
        row_index = find_or_create_row_for_date(date_str, sales_data)

        # Ensure local list is big enough
        if row_index - 1 >= len(sales_data):
            needed_rows = (row_index - len(sales_data))
            for _ in range(needed_rows):
                blank_row = [''] * max(1, len(headers))
                sales_data.append(blank_row)

        for fragrance_number_float, qty_sold in fragrance_dict.items():
            ensure_columns_for_fragrance(fragrance_number_float)
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
        print(f"Performing batch update of {len(cell_updates)} sales cells...")
        gspread_cells = [gspread.Cell(r, c, val) for (r, c, val) in cell_updates]
        safe_api_call(sales_sheet.update_cells, gspread_cells)
        print("Sales data updated.")
    else:
        print("No cells to update in sales data.")

##############################################################################
#                           Shopify Order Handling                           #
##############################################################################

def fetch_new_orders(start_date):
    """
    Fetch new orders from Shopify, created at or after `start_date`.
    """
    print(f"Fetching new orders from Shopify since {start_date}...")
    orders = []
    endpoint = f"{BASE_URL}/orders.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    params = {
        "created_at_min": start_date.isoformat(),
        "limit": 250,
        "status": "any"
    }

    while True:
        response = safe_api_call(requests.get, endpoint, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            fetched_orders = data["orders"]
            orders.extend(fetched_orders)
            print(f"Fetched {len(fetched_orders)} orders.")

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
            print(f"Failed to fetch orders. Status: {response.status_code}, Message: {response.text}")
            break

    print(f"Total orders fetched: {len(orders)}")
    return orders

def process_orders(orders, inventory, sold, already_processed_orders):
    """
    Process each order to update inventory & sold, build a sales log,
    and return a list of newly processed order IDs.
    """
    print("Processing orders...")
    new_processed_order_ids = []
    sales_log = {}

    for order in orders:
        order_id_str = str(order['id'])
        # Skip if already processed
        if order_id_str in already_processed_orders:
            continue

        print(f"\nProcessing Order ID: {order_id_str}")
        order_date_str = datetime.strptime(
            order['created_at'], "%Y-%m-%dT%H:%M:%S%z"
        ).date().strftime("%Y-%m-%d")

        for item in order['line_items']:
            title = item['title']
            quantity = item['quantity']

            # Skip samples
            if "sample" in title.lower():
                continue

            if "Fragrance Bundle" in title:
                print(f"Processing bundle: {title}")
                perfumes_processed = []
                for prop in item['properties']:
                    perfume_number = extract_perfume_number(prop['value'])
                    if perfume_number is not None and perfume_number in inventory:
                        inventory[perfume_number] -= quantity
                        sold[perfume_number] += quantity
                        sales_log.setdefault(order_date_str, {}).setdefault(perfume_number, 0)
                        sales_log[order_date_str][perfume_number] += quantity
                        perfumes_processed.append(perfume_number)
                        print(f"Perfume {perfume_number} => new inventory: {inventory[perfume_number]}")
                    else:
                        print(f"Perfume number '{prop['value']}' not found in inventory.")

                expected_count = 3 if "3x" in title else 2
                if len(perfumes_processed) != expected_count:
                    print(f"Warning: Expected {expected_count} in bundle, found {len(perfumes_processed)}.")
            else:
                perfume_number = extract_perfume_number(title)
                if perfume_number is not None and perfume_number in inventory:
                    inventory[perfume_number] -= quantity
                    sold[perfume_number] += quantity
                    sales_log.setdefault(order_date_str, {}).setdefault(perfume_number, 0)
                    sales_log[order_date_str][perfume_number] += quantity
                    print(f"Perfume {perfume_number} => new inventory: {inventory[perfume_number]}")
                else:
                    print(f"Perfume number for '{title}' not found in inventory.")

        # If we get here, we've processed this order
        new_processed_order_ids.append(order_id_str)

    # Batch update the main sheet (inventory/sold)
    print("Preparing to batch update inventory and sold in the main sheet...")
    sheet_values = safe_api_call(sheet.get_all_values)

    # Map perfume float => row index (skip the header row)
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

    for perfume_float, new_sold in sold.items():
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

    # Now update Blad2 (daily sales)
    if sales_log:
        update_sales_data(sales_log)
    else:
        print("No sales data to log in Blad2.")

    print("Order processing completed.")
    return new_processed_order_ids

##############################################################################
#                                   MAIN                                     #
##############################################################################

def main():
    try:
        print("Starting main process...")

        # 1. Ensure our table exists
        create_table_if_not_exists()

        # 2. Only process orders from 7 January 2025 at 13:02 onward, for example
        start_date = datetime(2025, 1, 7, 13, 2)

        # 3. Load processed order IDs from the database
        processed_orders = load_processed_orders_from_db()

        # 4. Get inventory and sold data from the main sheet
        inventory, sold_data = get_inventory_and_sold()

        # 5. Fetch new Shopify orders from the given start date
        orders = fetch_new_orders(start_date)
        if orders:
            # 6. Process them
            new_processed_ids = process_orders(orders, inventory, sold_data, processed_orders)

            # 7. Save newly processed order IDs back to the DB
            save_processed_orders_to_db(new_processed_ids)

            print("Inventory, sold amounts, and daily sales data updated.")
        else:
            print("No new orders found.")

    except Exception as e:
        print(f"Error in main process: {e}")
        print("No changes saved.")
    finally:
        print("Script ended:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":
    print("Starting script...")
    main()

