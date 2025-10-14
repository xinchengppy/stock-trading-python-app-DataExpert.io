import os
import time
import requests
from datetime import datetime
from dotenv import load_dotenv 
import snowflake.connector
import logging
load_dotenv()

# Configure logging
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = f"stock_job_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
LOG_FILE = os.path.join(LOG_DIR, log_filename)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'), 
    ]
)

logging.info("Logging started")

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000

# -------------------- MAIN STOCK JOB --------------------
def run_stock_job():
    DS = datetime.now().strftime('%Y-%m-%d')
    tickers = []
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        logging.error(f"Failed to fetch data from Polygon API: {e}", exc_info=True)
        return
    
    for ticker in data.get("results", []):
        ticker['ds'] = DS
        tickers.append(ticker)

    while 'next_url' in data:
        next_url = data['next_url']
        logging.info(f"Requesting next page: {next_url}")
        try:
            response = requests.get(next_url + f"&apiKey={POLYGON_API_KEY}")
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logging.error(f"Failed to fetch next page: {e}", exc_info=True)
            break

        for ticker in data.get("results", []):
            ticker['ds'] = DS
            tickers.append(ticker)
        time.sleep(12)

    logging.info(f"Fetched {len(tickers)} tickers from Polygon API.")

    # Infer schema from example
    example_ticker = tickers[0] if tickers else {}
    fieldnames = list(example_ticker.keys())

    # Load to Snowflake instead of CSV
    load_to_snowflake(tickers, fieldnames)
    logging.info(f"Loaded {len(tickers)} rows to Snowflake.")
    
# -------------------- SNOWFLAKE LOADER --------------------
def load_to_snowflake(rows, fieldnames):
    if not rows:
        logging.warning("No rows to insert into Snowflake.")
        return
    
    conn = None
    try:
        # Build connection kwargs
        connect_kwargs = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'session_parameters': {"CLIENT_TELEMETRY_ENABLED": False}
        }
        # Remove None values
        connect_kwargs = {k: v for k, v in connect_kwargs.items() if v}

        # Validate required fields
        required = ['user', 'password', 'account']
        for r in required:
            if r not in connect_kwargs:
                raise ValueError(f"Missing required connection parameter: {r}")
            
        # Read DB, schema and table_name from env
        database = os.getenv('SNOWFLAKE_DATABASE', 'STOCK_DB')
        schema = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
        table_name = os.getenv('SNOWFLAKE_TABLE', 'STOCK_TICKERS')

        # Connect to Snowflake
        try:
            conn = snowflake.connector.connect(**connect_kwargs)
            logging.info("Connected to Snowflake successfully.")
        except Exception as e:
            logging.error(f"Failed to connect to Snowflake: {e}", exc_info=True)
            return
        
        # Create structures & insert data
        try:
            with conn.cursor() as cs:
                # Create DB and schema if missing
                cs.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
                cs.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
                cs.execute(f"USE DATABASE {database}")
                cs.execute(f"USE SCHEMA {schema}")
                logging.info(f"Using {database}.{schema}")

                # Define typed schema
                type_overrides = {
                    'ticker': 'VARCHAR',
                    'name': 'VARCHAR',
                    'market': 'VARCHAR',
                    'locale': 'VARCHAR',
                    'primary_exchange': 'VARCHAR',
                    'type': 'VARCHAR',
                    'active': 'BOOLEAN',
                    'currency_name': 'VARCHAR',
                    'cik': 'VARCHAR',
                    'composite_figi': 'VARCHAR',
                    'share_class_figi': 'VARCHAR',
                    'last_updated_utc': 'TIMESTAMP_NTZ',
                    'ds': 'VARCHAR'
                }

                # CREATE TABLE
                columns_sql = ', '.join(
                    [f'"{col.upper()}" {type_overrides.get(col, "VARCHAR")}' for col in fieldnames]
                )
                qualified_table = f"{database}.{schema}.{table_name}"
                cs.execute(f'CREATE TABLE IF NOT EXISTS {qualified_table} ( {columns_sql} )')
                logging.info(f"Ensured table exists: {qualified_table}")

                # Prepare insert query
                column_list = ', '.join([f'"{c.upper()}"' for c in fieldnames])
                placeholders = ', '.join([f'%({c})s' for c in fieldnames])
                insert_sql = f'INSERT INTO {qualified_table} ( {column_list} ) VALUES ( {placeholders} )'
                # Transform rows to dictionary format
                transformed = [{k: row.get(k) for k in fieldnames} for row in rows]

                # Insert data
                if transformed:
                    cs.executemany(insert_sql, transformed)
                    conn.commit()
                    logging.info(f"Inserted {len(transformed)} rows into {qualified_table}")
                else:
                    logging.warning("No data to insert after transformation.")

        except Exception as e:
            logging.error(f"Failed to load data to Snowflake: {e}", exc_info=True)
            raise

    finally:
        if conn:
            try:
                conn.close()
                logging.info("Snowflake connection closed.")
            except Exception as e:
                logging.warning(f"Error closing Snowflake connection: {e}", exc_info=True)

if __name__ == '__main__':
    run_stock_job()
    logging.info("Script finished")
