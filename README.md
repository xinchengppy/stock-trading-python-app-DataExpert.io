# Stock Trading Python App — Snowflake Data Load

This project demonstrates how to extract stock ticker data, process it in Python, and load it into a Snowflake data warehouse using the official **Snowflake Python Connector**.  
It was created as part of the [DataExpert.io](https://github.com/DataExpert-io-Community) [Data Engineering bootcamp](https://github.com/DataExpert-io/data-engineer-handbook).


## Project Structure
```
stock-trading-python-app-DataExpert.io/
├── script.py                # Main script: loads data into Snowflake
├── .env                     # Example environment variable file
├── requirements.txt         # Python dependencies
├── logs/                    # Log files (auto-generated)
└── README.md                # Documentation
```

## Requirements
- **Python ≥ 3.10**
- A valid **Snowflake account** (trial or permanent)
- Install dependencies:
  ```bash
  pip install -r requirements.txt
  ```
  ⚠️ Important: Snowflake Trial Limitation
- Snowflake’s free trial lasts 30 days and includes $400 in usage credits.
- After the trial expires, your virtual warehouse and databases will be suspended. To continue running this project:
1. Create a new trial account with another email, or
2. Modify the code to load data into a different database (e.g. PostgreSQL, SQLite, BigQuery, etc.)

## Environment Variables
Create a .env file in the project root using .env.example as reference:
```
POLYGON_API_KEY = your_key
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account       # Found in your Snowflake login URL
```

## How to Run
Run the script:
```
python3 script.py
```
and you are good to go.
The script will automatically:
- Fetch stock ticker data from the Polygon.io API
- Connect to Snowflake using credentials in your .env
- Automatically create the database, schema, and table if they don’t exist
- Insert all retrieved ticker records into the specified Snowflake table

Every execution of the script automatically generates a timestamped log file inside the /logs folder:
```
logs/
├── stock_job_2025-10-13_20-45-10.log
├── stock_job_2025-10-13_22-15-32.log
└── ...
```


## Verify Data in snowflake
Log in to your Snowflake web console and run:
```
USE DATABASE STOCK_DB;
USE SCHEMA PUBLIC;
SELECT * FROM STOCK_TICKERS LIMIT 10;
```
You should see the inserted stock ticker data.

## Run again on another day
Each time you rerun the script, it will fetch the latest tickers from Polygon.io
and insert them with a new ds (date stamp = another day) into the same Snowflake table.



***
This project is for personal learning purposes.

This project is guided by:
[DataExpert.io](https://www.dataexpert.io/) and [Zach Wilson](https://www.linkedin.com/in/eczachly/)


