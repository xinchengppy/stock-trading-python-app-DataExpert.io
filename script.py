import os
import time
import requests
from datetime import datetime
from dotenv import load_dotenv 
import snowflake.connector
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000

def run_stock_job():
    DS = datetime.now().strftime('%Y-%m-%d')
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    tickers = []

    data = response.json()
    for ticker in data["results"]:
        ticker['ds'] = DS
        tickers.append(ticker)

    while 'next_url' in data:
        print("requesting next page", data['next_url'])
        response = requests.get(data['next_url'] + f"&apiKey={POLYGON_API_KEY}")
        data = response.json()
        for ticker in data["results"]:
            ticker['ds'] = DS
            tickers.append(ticker)
        time.sleep(12)

    example_ticker = {'ticker': 'FIXD', 
    'name': 'First Trust Exchange-Traded Fund VIII First Trust Smith Opportunistic Fixed Income ETF', 
    'market': 'stocks', 
    'locale': 'us', 
    'primary_exchange': 'XNAS', 
    'type': 'ETF', 
    'active': True, 
    'currency_name': 'usd', 
    'cik': '0001667919', 
    'composite_figi': 'BBG00FZ4KFH5', 
    'share_class_figi': 'BBG00FZ4KG74', 
    'last_updated_utc': '2025-09-29T06:04:58.488325301Z',
    'ds': DS
    }

    fieldnames = list(example_ticker.keys())
    # Load to Snowflake instead of CSV
    load_to_snowflake(tickers, fieldnames)
    print(f'Loaded {len(tickers)} rows to Snowflake')

    
def load_to_snowflake(rows, fieldnames):
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
        
    # Read DB and schema from env
    database = os.getenv('SNOWFLAKE_DATABASE', 'STOCK_DB')
    schema = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')

    conn = snowflake.connector.connect(**connect_kwargs)
    try:
        with conn.cursor() as cs:
            #create DB and schema if missing
            cs.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            cs.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
            cs.execute(f"USE DATABASE {database}")
            cs.execute(f"USE SCHEMA {schema}")

            table_name = os.getenv('SNOWFLAKE_TABLE', 'STOCK_TICKERS')

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

            # INSERT
            column_list = ', '.join([f'"{c.upper()}"' for c in fieldnames])
            placeholders = ', '.join([f'%({c})s' for c in fieldnames])
            insert_sql = f'INSERT INTO {table_name} ( {column_list} ) VALUES ( {placeholders} )'

            transformed = [{k: row.get(k) for k in fieldnames} for row in rows]

            if transformed:
                cs.executemany(insert_sql, transformed)
                print(f"Inserted {len(transformed)} rows into {table_name}")

    finally:
        conn.close()

if __name__ == '__main__':
    run_stock_job()
