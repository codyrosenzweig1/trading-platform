import yfinance as yf #Used to fetch stock data
import pandas as pd 
from sqlalchemy import create_engine #Import sqlAlchemy to interact with the db
from dotenv import load_dotenv
import os

#load variables from .env
load_dotenv()

# Configure the db connection
DATABASE_URI = f"postgresql+psycopg2://postgres:{os.getenv('DATABASE_PASSWORD')}@localhost/stock_trading"

engine = create_engine(DATABASE_URI)

try:
    engine = create_engine(DATABASE_URI)
    with engine.connect() as connection:
        print("Database connection successful!")
except Exception as e:
    print(f"Error connecting to database: {e}")


def fetch_stock_data(ticker, start_date, end_date):
    """
    Fetch historical data for a given ticket and save it to database

    Args:
        ticker (str): The stock ticker symbol ('AAPL' for apple)
        start_date (str): Start date for the data in 'YYYY_MM_DD'
        end_date (str): End date for the data in 'YYYY_MM_DD'
    
    Returns:
        DataFrame: The fetched stock data
    """
    # Fetch stock data from Yahoo finance
    stock = yf.Ticker(ticker) # Create a ticker object for the given stock
    data = stock.history(start=start_date, end=end_date) # fetch historical data

    # Add a column for the stock ticker symbol
    data['Ticker'] = ticker
    
    # Reset the index so the 'Date' column becomes a regular column
    data.reset_index(inplace=True)
    
    # Save data to the database
    try:
        data.to_sql('stock_data', engine, if_exists='append', index=False)
    except Exception as e:
        print(f"Error saving data to database: {e}")
    
    return data

if __name__ == "__main__":
    # Retreive the info we want
    df = fetch_stock_data("AAPL", "2022-01-01", "2022-12-31")
    print(df.head())