import requests
from bs4 import BeautifulSoup
import mysql.connector
import datetime

def get_previous_close_from_google(ticker):
    url = f'https://www.google.com/finance/quote/{ticker}'
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for HTTP request issues
    except requests.RequestException as e:
        print(f"Error fetching data from Google Finance: {e}")
        return 'Not available'
    
    soup = BeautifulSoup(response.text, 'html.parser')
    price_div = soup.find('div', {'class': 'YMlKec fxKbKc'})
    if not price_div:
        print(f"Price div not found for ticker {ticker}. HTML structure may have changed.")
        return 'Not available'
    
    price = price_div.text.strip()
    return price

def clean_price_string(price_string):
    # Remove any non-numeric characters except the decimal point
    cleaned_price = ''.join(c for c in price_string if c.isdigit() or c == '.')
    return cleaned_price

def update_stock_prices():
    # Connect to MySQL
    try:
        db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="sohel7866",
            database="stock_pricing_application"
        )
        print("Connected to the database successfully.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return

    cursor = db.cursor()

    # Get today's date
    current_date = datetime.datetime.today().strftime('%Y_%m_%d')

    # Create a new column in g_stock_price for today's date if it doesn't exist
    column_name = f"close_{current_date}"
    cursor.execute(f"SHOW COLUMNS FROM g_stock_price LIKE '{column_name}'")
    if not cursor.fetchone():
        cursor.execute(f"ALTER TABLE g_stock_price ADD COLUMN `{column_name}` FLOAT")
        print(f"Added '{column_name}' column to g_stock_price table.")

    # Fetch G_Ticker and security_id from stock_info table
    cursor.execute("SELECT security_id, G_Ticker FROM stock_info_new WHERE SRC = 'GF'")
    tickers = cursor.fetchall()
    print(f"Fetched {len(tickers)} tickers from the stock_info_new table.")

    # Fetch and update the latest Previous Close price in g_stock_price table
    for ticker in tickers:
        security_id = ticker[0]
        g_ticker = ticker[1]
        try:
            # Fetch the stock data from Google Finance
            stock_price = get_previous_close_from_google(g_ticker)
            if stock_price != 'Not available':
                # Clean and convert the price
                clean_price = clean_price_string(stock_price)
                try:
                    close_price = float(clean_price)
                    print(f"Fetched Previous Close {close_price} for Google Ticker {g_ticker}")

                    # Update the g_stock_price table with the Close price for today's date
                    cursor.execute(f"UPDATE g_stock_price SET `{column_name}` = %s WHERE security_id = %s", 
                                   (close_price, security_id))
                except ValueError as ve:
                    print(f"ValueError converting price for Google Ticker {g_ticker}: {clean_price} - {ve}")
            else:
                print(f"No data found for Google Ticker {g_ticker}")
        except Exception as e:
            print(f"Error fetching or updating data for Google Ticker {g_ticker}: {e}")

    # Commit the changes to the database
    try:
        db.commit()
        print("Database updated successfully.")
    except mysql.connector.Error as err:
        print(f"Error committing changes: {err}")

    # Close the database connection
    db.close()
    print("Database connection closed.")

# Example usage
update_stock_prices()
