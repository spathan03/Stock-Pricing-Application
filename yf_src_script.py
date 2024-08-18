import mysql.connector
import yfinance as yf
import datetime

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
    exit(1)

cursor = db.cursor()

# Get today's date
current_date = datetime.datetime.today().strftime('%Y_%m_%d')

# Create a new column in y_stock_price for today's date if it doesn't exist
column_name = f"close_{current_date}"
cursor.execute(f"SHOW COLUMNS FROM y_stock_price LIKE '{column_name}'")
if not cursor.fetchone():
    cursor.execute(f"ALTER TABLE y_stock_price ADD COLUMN `{column_name}` FLOAT")
    print(f"Added '{column_name}' column to y_stock_price table.")

# Fetch Y_Ticker and security_id from stock_info table
cursor.execute("SELECT security_id, Y_Ticker FROM stock_info WHERE SRC LIKE 'YF'")
tickers = cursor.fetchall()
print(f"Fetched {len(tickers)} tickers from the stock_info table.")

# Fetch and update the latest Close price in y_stock_price table
for ticker in tickers:
    security_id = ticker[0]
    y_ticker = ticker[1]
    try:
        # Fetch the stock data from Yahoo Finance
        stock = yf.Ticker(y_ticker)
        hist = stock.history(period='1d')
        if not hist.empty:
            close_price = float(hist['Close'].iloc[0])
            print(f"Fetched Close {close_price} for Y_Ticker {y_ticker}")

            # Update the y_stock_price table with the Close price for today's date
            cursor.execute(f"UPDATE y_stock_price SET `{column_name}` = %s WHERE security_id = %s", 
                           (close_price, security_id))
        else:
            print(f"No data found for Y_Ticker {y_ticker}")
    except Exception as e:
        print(f"Error fetching or updating data for Y_Ticker {y_ticker}: {e}")

# Commit the changes to the database
try:
    db.commit()
    print("Database updated successfully.")
except mysql.connector.Error as err:
    print(f"Error committing changes: {err}")

# Close the database connection
db.close()
print("Database connection closed.")
