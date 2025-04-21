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

Provides daily historical Bitcoin prices and volumes in various markets (e.g., USD). The API function `DIGITAL_CURRENCY_DAILY` returns time-series JSON with open, high, low, close, and volume data for the past 350 days.

**Sample response:**

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

### Gold API - [APISED](https://apised.com/)

Returns live gold prices in different karats and exchange rates for multiple currencies against a base currency. The data includes detailed price metrics and currency conversion values.

**Sample response:**

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

## Project Structure
```
etl-project-amdaris-2025/
├── etl/
│   ├── commons/
│   │   ├── database.py               # Base database connector
│   │   └── logger.py                 # Logging configuration
│   ├── extract/
│   │   ├── btc_extract.py            # Bitcoin API client
│   │   ├── gold_extract.py           # Gold API client
│   │   ├── database_extract.py       # Extract database operations
│   │   ├── utils_extract.py          # Extract utilities
│   │   ├── logger_extract.py         # Extract-specific logger
│   │   └── main_extract.py           # Extract process entry point
│   ├── transform/
│   │   ├── btc_transform.py          # Bitcoin data transformation
│   │   ├── gold_transform.py         # Gold data transformation
│   │   ├── database_transform.py     # Transform database operations
│   │   ├── utils_transform.py        # Transform utilities
│   │   ├── logger_transform.py       # Transform-specific logger
│   │   └── main_transform.py         # Transform process entry point
│   └── load/
│       ├── btc_load.py               # Bitcoin data loading
│       ├── gold_load.py              # Gold data loading
│       ├── database_load.py          # Load database operations
│       ├── logger_load.py            # Load-specific logger
│       └── main_load.py              # Load process entry point
└── run.py                            # Run the application
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
mysql -u username -p password < transform/schema_extract.sql
mysql -u username -p password < load/schema_extract.sql
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