import pandas as pd
import numpy as np
import logging
import sys
import os
from sqlalchemy import create_engine, exc
from urllib.parse import quote_plus
from dotenv import load_dotenv

# ---------------------------------------------------------
# 1. SETUP LOGGING
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 2. CONFIGURATION & VALIDATION
# ---------------------------------------------------------
def load_config():
    """Loads environment variables and ensures required keys exist."""
    load_dotenv()
    required_vars = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME', 'STAGING_DB_NAME']
    config = {}
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            logger.error(f"Critical environment variable missing: {var}")
            sys.exit(1)
        config[var] = value
        
    config['USD_TO_INR_RATE'] = float(os.getenv('USD_TO_INR_RATE', 82.0)) 
    return config

# ---------------------------------------------------------
# 3. DATABASE EXTRACTION
# ---------------------------------------------------------
def get_db_engine(config, target_db=None):
    """Creates and verifies the database connection."""
    db_to_use = target_db if target_db else config['DB_NAME']
    
    try:
        connection_string = f"mysql+mysqlconnector://{config['DB_USER']}:{quote_plus(config['DB_PASSWORD'])}@{config['DB_HOST']}:{config['DB_PORT']}/{db_to_use}"
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            logger.info(f"Successfully connected to database: {db_to_use}")
        return engine
    except exc.SQLAlchemyError as e:
        logger.error(f"Failed to connect to the database ({db_to_use}): {e}")
        sys.exit(1)

def extract_table(engine, table_name):
    """Extracts a table and handles potential SQL errors gracefully."""
    logger.info(f"Extracting table: {table_name}...")
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name};", engine)
        logger.info(f"Successfully extracted {len(df)} rows from {table_name}.")
        return df
    except Exception as e:
        logger.error(f"Error extracting {table_name}: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------
# 4. TRANSFORMATION LOGIC
# ---------------------------------------------------------
def transform_customers(df):
    if df.empty:
        return df
    logger.info("Cleaning customers table...")
    try:
        df = df.copy()
        if 'customer_code' in df.columns:
            df['customer_code'] = df['customer_code'].str.strip().str.upper()
        # Fix column typo
        df.rename(columns={'custmer_name': 'customer_name'}, inplace=True)
        return df
    except Exception as e:
        logger.error(f"Error transforming customers: {e}")
        return df

def transform_products(df):
    if df.empty:
        return df
    logger.info("Cleaning products table...")
    try:
        df = df.copy()
        if 'product_code' in df.columns:
            df['product_code'] = df['product_code'].str.strip().str.upper()
        return df
    except Exception as e:
        logger.error(f"Error transforming products: {e}")
        return df

def transform_markets(df):
    if df.empty:
        return df
        
    logger.info("Cleaning markets table...")
    try:
        df = df.copy()
        if 'zone' in df.columns:
            df['zone'] = df['zone'].replace(r'^\s*$', np.nan, regex=True)
            df['zone'] = df['zone'].fillna('Overseas')
            
        if 'markets_code' in df.columns:
            df['markets_code'] = df['markets_code'].str.strip().str.upper()
            
        # Rename columns to standard singular format
        df.rename(columns={'markets_code': 'market_code', 'markets_name': 'market_name'}, inplace=True)
        return df
    except Exception as e:
        logger.error(f"Error transforming markets: {e}")
        return df

def transform_dates(df, date_col, prefix=None):
    if df.empty or date_col not in df.columns:
        return df
        
    logger.info(f"Transforming dates for column: {date_col}")
    try:
        df = df.copy() 
        df[date_col] = pd.to_datetime(df[date_col])
        pre = prefix if prefix else date_col
        
        df[f'{pre}_day'] = df[date_col].dt.day
        df[f'{pre}_day_of_week'] = df[date_col].dt.dayofweek
        df[f'{pre}_day_name'] = df[date_col].dt.day_name()
        df[f'{pre}_month'] = df[date_col].dt.month
        df[f'{pre}_month_name'] = df[date_col].dt.month_name()
        df[f'{pre}_year'] = df[date_col].dt.year
        return df
    except Exception as e:
        logger.warning(f"Failed to transform dates for {date_col}: {e}")
        return df

def transform_transactions(df, exchange_rate):
    if df.empty:
        return df
        
    logger.info("Cleaning transactions table...")
    try:
        df = df.copy()
        df['order_date'] = pd.to_datetime(df['order_date'])
        
        initial_rows = len(df)
        df = df[df['sales_amount'] >= 0]
        logger.info(f"Filtered out {initial_rows - len(df)} negative transaction rows.")

        usd_mask = df['currency'].str.strip().str.upper() == 'USD'
        df.loc[usd_mask, 'sales_amount'] *= exchange_rate
        df.loc[usd_mask, 'currency'] = 'INR'
        
        # Clean relational codes
        cols_to_clean = ['product_code', 'customer_code', 'market_code']
        for col in cols_to_clean:
            if col in df.columns:
                df[col] = df[col].str.strip().str.upper()
                
        return df
    except Exception as e:
        logger.error(f"Error transforming transactions: {e}")
        return df

# ---------------------------------------------------------
# 5. MAIN EXECUTION
# ---------------------------------------------------------
def main():
    logger.info("Starting Sales Analytics Pipeline...")
    
    # 1. Setup & Configuration
    config = load_config()
    source_engine = get_db_engine(config) 
    
    # 2. Extract Data
    df_customers = extract_table(source_engine, "customers")
    df_markets = extract_table(source_engine, "markets")
    df_products = extract_table(source_engine, "products")
    df_transactions = extract_table(source_engine, "transactions")
    df_dates = extract_table(source_engine, "date")
    
    # 3. Transform Data
    logger.info("Starting data transformations...")
    
    # Applied the new transformation functions
    df_customers_clean = transform_customers(df_customers)
    df_products_clean = transform_products(df_products)
    df_markets_clean = transform_markets(df_markets)
    df_transactions_clean = transform_transactions(df_transactions, config['USD_TO_INR_RATE'])
    
    df_dates_clean = transform_dates(df_dates, 'date')
    df_dates_clean = transform_dates(df_dates_clean, 'cy_date', prefix='cy')

    if not df_dates_clean.empty:
        try:
            ordered_cols = [
                'date', 'date_day', 'date_day_of_week', 'date_day_name', 'date_month', 'date_month_name', 'date_year',
                'cy_date', 'cy_day', 'cy_day_of_week', 'cy_day_name', 'cy_month', 'cy_month_name', 'cy_year'
            ]
            ordered_cols = [c for c in ordered_cols if c in df_dates_clean.columns]
            df_dates_clean = df_dates_clean[ordered_cols]
        except Exception as e:
            logger.warning(f"Could not reorder date columns: {e}")

    logger.info("Transformations complete. Preparing to load...")
    
    # ---------------------------------------------------------
    # 4. LOAD PHASE
    # ---------------------------------------------------------
    staging_engine = get_db_engine(config, target_db=config['STAGING_DB_NAME'])
    logger.info("Loading cleaned data into staging database...")

    load_params = {
        'con': staging_engine, 
        'if_exists': 'replace', 
        'index': False, 
        'chunksize': 1000
    }

    try:
        df_customers_clean.to_sql('customers_clean', **load_params)
        df_markets_clean.to_sql('markets_clean', **load_params)
        df_products_clean.to_sql('products_clean', **load_params)
        df_transactions_clean.to_sql('transactions_clean', **load_params)
        df_dates_clean.to_sql('dates_clean', **load_params)
        logger.info("Data successfully loaded into staging layer.")
    except Exception as e:
        logger.error(f"Failed to load data into staging database: {e}")

if __name__ == "__main__":
    main()