# Bitcoin & Gold Prices ETL Pipeline

This project implements an ETL pipeline for Bitcoin and Gold price data for different currencies. 
The pipeline extracts data from external APIs, transforms it into a structured format, and loads it into a data warehouse for analysis.

## Table of Contents
- [Overview](#overview)

- [API & Data](#api--data)
  - [Bitcoin API](#bitcoin-api---alpha-vantage)
  - [Gold API](#gold-api---apised)

- [Project Structure](#project-structure)

- [Database Schema](#database-schema)

- [Process Flows](#process-flows)
  - [Extract Process Flow](#extract-process-flow)
  - [Transform Process Flow](#transform-process-flow)
  - [Load Process Flow](#load-process-flow)

- [Setup](#setup)

- [Usage](#usage)

- [Data Visualizations](#data-visualizations)
  - [ETL Performance](#etl-performance-dashboards)
  - [Financial Data Analysis](#financial-data-analysis)

## Overview
This ETL pipeline collects Bitcoin and Gold price data for multiple currencies, processes it, and stores it in a structured data warehouse. The system is designed to be efficient, maintainable, and scalable, with comprehensive logging at each stage of the process.

Key features:

- Extracts data from Bitcoin and Gold API
- Transforms raw data into structured formats
- Loads data into a **star-schema** data warehouse
- Maintains audit trails through logging
- Optimized to process only new or changed data

## API & Data

This project uses two external APIs.

### Bitcoin API - [Alpha Vantage](https://www.alphavantage.co)

Alpha Vantage provides daily historical Bitcoin prices and volumes in various currency markets. This project uses the `DIGITAL_CURRENCY_DAILY` function to retrieve time-series data.

**Endpoint:**
```
https://www.alphavantage.co/query
```
**Request Parameters:**

| Parameter     | Description                | Example Value          |
|---------------|----------------------------|------------------------|
| **function**  | API function to call       | DIGITAL_CURRENCY_DAILY |
| **symbol**    | Digital currency symbol    | BTC                    |
| **market**    | Market/currency code       | USD                    |
| **apikey**    | Your Alpha Vantage API key | btc_api_key            |

**Sample Request:**

```
https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_DAILY&symbol=BTC&market=USD&apikey=btc_api_key
```

**Successful Response**

The API returns a JSON object containing metadata and daily price data for the past 350 days:

```json
{
  "Meta Data": {
    "1. Information": "Daily Prices and Volumes for Digital Currency",
    "2. Digital Currency Code": "BTC",
    "3. Digital Currency Name": "Bitcoin",
    "4. Market Code": "USD",
    "5. Market Name": "United States Dollar",
    "6. Last Refreshed": "2025-04-17 00:00:00",
    "7. Time Zone": "UTC"
  },
  "Time Series (Digital Currency Daily)": {
    "2025-04-17": {
      "1. open": "84028.71000000",
      "2. high": "84118.21000000",
      "3. low": "83924.19000000",
      "4. close": "84023.41000000",
      "5. volume": "106.89082648"
    },
    "2025-04-16": {
      "1. open": "83622.52000000",
      "2. high": "85526.40000000",
      "3. low": "83088.02000000",
      "4. close": "84028.72000000",
      "5. volume": "8243.05901273"
    },
    {...}
  }
}
```
**Error Response**

Alpha Vantage returns an error message if the request is invalid, but with an HTTP response code 200:

```json
{
    "Error Message": "Invalid API call. Please retry or visit the documentation (https://www.alphavantage.co/documentation/) for DIGITAL_CURRENCY_DAILY."
}
```


### Gold API - [APISED](https://apised.com/)

APISED's Gold API provides real-time gold prices in different karats and exchange rates for multiple currencies.

**Endpoint:**
```
https://gold.g.apised.com/v1/latest
```
**Request Parameters:**

| Parameter         | Description                                           | Example Value                           |
|-------------------|-------------------------------------------------------|-----------------------------------------|
| **metals**        | Metal code to retrieve                                | XAU                                     |
| **base_currency** | Base currency for prices                              | USD                                     |
| **weight_unit**   | Unit of weight                                        | gram                                    |
| **currencies**    | Comma-separated list of currencies for exchange rates | USD,EUR,GBP,AED,BGN,CAD,CHF,RON,TRY,UAH |

**Headers:**

| Header        | Description         | Example Value |
|---------------|---------------------|---------------|
| **x-api-key** | Your APISED API key | gold_api_key  |

**Sample Request:**

```
https://gold.g.apised.com/v1/latest?metals=XAU&base_currency=GBP&weight_unit=gram&currencies=EUR,RON,GBP,USD,TRY,CAD,CHF,UAH,BGN,AED
```

**Successful Response**

```json
{
    "status": "success",
    "data": {
        "timestamp": 1744891132838,
        "base_currency": "USD",
        "metals": "XAU",
        "weight_unit": "gram",
        "weight_name": "Gram (g)",
        "metal_prices": {
            "XAU": {
                "open": 107.65951,
                "high": 107.95482,
                "low": 106.50739,
                "prev": 107.48301,
                "change": -0.71214,
                "change_percentage": -0.66147,
                "price": 106.94737,
                "ask": 106.95606,
                "bid": 106.93869,
                "price_24k": 106.94737,
                "price_22k": 98.03866,
                "price_21k": 93.57895,
                "price_20k": 89.11925,
                "price_18k": 80.21053,
                "price_16k": 71.30181,
                "price_14k": 62.3824,
                "price_10k": 44.56497
            }
        },
        "currency_rates": {
            "AED": 3.67299,
            "BGN": 1.71965,
            "CAD": 1.39033,
            "CHF": 0.81715,
            "EUR": 0.88037,
            "GBP": 0.7559,
            "RON": 4.3815,
            "TRY": 38.08463,
            "UAH": 41.41769,
            "USD": 1
        }
    }
}
```

**Error Response**

The Gold API returns a 400 status code with a descriptive error message when parameters are invalid:

```json
{
    "status": "fail",
    "message": [
        "currencies: currencies parameter contains a value that is not supported"
    ]
}
```

## Project Structure
```
etl-project-amdaris-2025/
├── data/
│   ├── error/                        # Contains JSON files with failed records
│   │   ├── bitcoin/                  # Bitcoin data
│   │   └── gold/                     # Gold data
│   ├── processed/                    # Contains JSON files with processed records
│   │   ├── bitcoin/                  # Bitcoin data
│   │   └── gold/                     # Gold data
│   ├── raw/                          # Contains JSON files with raw API responses
│   │   ├── bitcoin/                  # Bitcoin data
│   │   └── gold/                     # Gold data
├── etl/
│   ├── commons/
│   │   ├── database.py               # Base database connector (shared)
│   │   └── logger.py                 # Logging configuration (shared)
│   ├── extract/
│   │   ├── btc_extract.py            # Bitcoin API client
│   │   ├── gold_extract.py           # Gold API client
│   │   ├── database_extract.py       # Extract database operations
│   │   ├── utils_extract.py          # Extract helper functions
│   │   ├── logger_extract.py         # Extract-specific logger setup
│   │   ├── main_extract.py           # Extract process entry point
│   │   ├── schema_extract.sql        # SQL schema for extract stage
│   │   └── logs/                     # Log files generated during Extract step
│   ├── transform/
│   │   ├── btc_transform.py          # Bitcoin data transformation logic
│   │   ├── gold_transform.py         # Gold data transformation logic
│   │   ├── database_transform.py     # Transform database operations
│   │   ├── utils_transform.py        # Transform helper functions
│   │   ├── logger_transform.py       # Transform-specific logger setup
│   │   ├── main_transform.py         # Transform process entry point
│   │   ├── schema_transform.sql      # SQL schema for transform stage
│   │   └── logs/                     # Log files generated during Transform step
│   └── load/
│       ├── btc_load.py               # Load Bitcoin data to DB
│       ├── gold_load.py              # Load Gold data to DB
│       ├── database_load.py          # Load database operations
│       ├── logger_load.py            # Load-specific logger setup
│       ├── main_load.py              # Load process entry point
│       ├── schema_load.sql           # SQL schema for load stage
│       └── logs/                     # Log files generated during Load step
├── .env                              # Environment variables
├── .gitignore                        # Git ignored files
├── README.md                         # Project description and usage
├── requirements.txt                  # Python dependencies
└── run.py                            # Entry point to run the ETL pipeline
```

## Database Schema
The database is structured into 3 schemas corresponding to each step of ETL.

![Entity-Relationship Diagram](https://i.imgur.com/rZgPaa1.png)

**Extract Schema**
- `api`: API source reference table
- `api_import_log`: Log of API calls
- `import_log`: Log of imported JSON files

**Transform Schema**
- `transform_log`: Log of processed files
- `btc_data_import`: Staging table for Bitcoin data
- `gold_data_import`: Staging table for Gold data

**Warehouse Schema**
- `dim_date`: Date dimension
- `dim_currency`: Currency dimension
- `fact_btc`: Bitcoin price facts
- `fact_gold`: Gold price facts
- `fact_exchange_rates`: Currency exchange rates facts

## Process Flows

### Extract Process Flow

![Extract Flow Diagram](https://i.imgur.com/qHZkst3.png)

### Transform Process Flow

![Transform Flow Diagram](https://i.imgur.com/Vmj7RQE.png)

### Load Process Flow

![Load Flow Diagram](https://i.imgur.com/wBojxz9.png)

## Setup

### 1. Clone the repository: 

```commandline
git clone https://github.com/anutzaa/etl-project-amdaris-2025.git
cd etl-project-amdaris-2025
```

### 2. Set up a virtual environment and environment variables:

```commandline
python -m venv venv
.\venv\Scripts\activate
type nul >.env
```

### 3. Set API keys:

```dotenv
# Get a free key from https://www.alphavantage.co/support/#api-key
BTC_API_KEY=btc_api_key

# Get a free key from https://apised.com/
GOLD_API_KEY=gold_api_key

# Set MySQL connection information
DB_HOST=host
DB_PORT=port
DB_DATABASE=database
DB_USER=user
DB_PASSWORD=password
```

### 4. Install dependencies:

```commandline
pip install -r requirements.txt
```

### 5. Set up the database:
```commandline
mysql -u username -p password < extract/schema_extract.sql
mysql -u username -p password < transform/schema_transform.sql
mysql -u username -p password < load/schema_load.sql
```

## Usage

Run the complete ETL process:
```commandline
python -m run all
```
To run ETL steps independently:
  - Extract:
  ```commandline
    python -m run extract
   ```
  - Transform:
  ```commandline
    python -m run transform
   ```
  - Load:
  ```commandline
    python -m run load
   ```

## Data Visualizations

The project includes data visualization dashboards built in **Power BI** to monitor ETL performance and analyze Bitcoin and Gold price data.

### ETL Performance Dashboards

The Power BI dashboards provide monitoring of key performance indicators (KPIs) for the ETL pipeline:

- **Extract Metrics**: API response times, success rates, and data volume trends
- **Transform Metrics**: Processing times, error rates, and data quality indicators

### Financial Data Analysis

The Power BI reports also include visualizations of Bitcoin and Gold price data:

- **Price Trends**: Historical price movements across different currencies
- **Correlation Analysis**: Relationship between Bitcoin and Gold prices
- **Currency Comparisons**: How prices vary across different base currencies