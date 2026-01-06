# Exchange Rates Data Pipeline

A data engineering portfolio project demonstrating an incremental data pipeline using **Snowflake** and **dbt**. This project extracts currency exchange rate data from the Frankfurter API, loads it into Snowflake, and transforms it using dbt with incremental loading patterns.

---

## Project Overview

**Objective:** Build an end-to-end data pipeline that:
1. Extracts exchange rate data from a public API
2. Loads raw JSON into Snowflake
3. Transforms data using dbt (staging → fact layers)
4. Implements incremental loading to process only new data

**Key Concept Learned:** Incremental Models in dbt

---

## Architecture
```
┌─────────────────────┐
│   Frankfurter API   │
│  (Exchange Rates)   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Python Script     │
│  (load_raw_data.py) │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│     Snowflake       │
│  RAW_EXCHANGE_RATES │
│    (VARIANT/JSON)   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│        dbt          │
│  ┌───────────────┐  │
│  │    Staging    │  │
│  │stg_exchange_  │  │
│  │    rates      │  │
│  └───────┬───────┘  │
│          │          │
│          ▼          │
│  ┌───────────────┐  │
│  │     Marts     │  │
│  │fct_exchange_  │  │
│  │rates (INCR)   │  │
│  └───────────────┘  │
└─────────────────────┘
```

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Data Warehouse | Snowflake |
| Transformation | dbt Core |
| Data Extraction | Python (requests library) |
| Data Source | Frankfurter API |
| Version Control | Git/GitHub |

---

## Project Structure
```
de_exchange_rates_pipeline/
├── models/
│   ├── staging/
│   │   ├── stg_exchange_rates.sql    # Flattens JSON to rows
│   │   └── schema.yml                 # Tests & documentation
│   └── marts/
│       ├── fct_exchange_rates.sql    # Incremental fact table
│       └── schema.yml                 # Tests & documentation
├── load_raw_data.py                   # Python script to load API data
├── dbt_project.yml                    # dbt project configuration
└── README.md
```

---

## Step-by-Step Implementation

### Phase 1: Environment Setup

#### 1.1 Snowflake Setup
Created database and schema to store exchange rate data:
```sql
CREATE OR REPLACE DATABASE DE_EXCHANGE_RATES;
CREATE OR REPLACE SCHEMA DE_EXCHANGE_RATES.MAIN;
```

#### 1.2 dbt Installation
Installed dbt with Snowflake adapter:
```bash
pip3 install dbt-snowflake
```

#### 1.3 dbt Project Initialization
```bash
dbt init de_exchange_rates_pipeline
```

#### 1.4 Profile Configuration
Configured `~/.dbt/profiles.yml` to connect to Snowflake:
```yaml
de_exchange_rates_pipeline:
  outputs:
    dev:
      account: <your-account>
      database: DE_EXCHANGE_RATES
      password: <your-password>
      role: ACCOUNTADMIN
      schema: MAIN
      threads: 1
      type: snowflake
      user: <your-username>
      warehouse: COMPUTE_WH
  target: dev
```

#### 1.5 Test Connection
```bash
dbt debug
```

---

### Phase 2: Data Extraction & Loading

#### 2.1 Explore the Data Source
Frankfurter API provides free exchange rate data. Example API call:
```
https://api.frankfurter.app/2024-01-01..2024-06-30?from=USD
```

Returns JSON with nested structure:
```json
{
  "amount": 1,
  "base": "USD",
  "rates": {
    "2024-01-02": {"EUR": 0.9, "GBP": 0.8, ...},
    "2024-01-03": {"EUR": 0.91, "GBP": 0.81, ...}
  }
}
```

#### 2.2 Create Raw Table
Created table with VARIANT column to store raw JSON:
```sql
CREATE OR REPLACE TABLE DE_EXCHANGE_RATES.MAIN.RAW_EXCHANGE_RATES (
    RAW_DATA VARIANT,
    LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

#### 2.3 Python Load Script
Created `load_raw_data.py` to extract from API and load into Snowflake:
```python
import requests
import snowflake.connector
import json

# Snowflake connection
conn = snowflake.connector.connect(
    user='<username>',
    password='<password>',
    account='<account>',
    warehouse='COMPUTE_WH',
    database='DE_EXCHANGE_RATES',
    schema='MAIN'
)

# Pull exchange rates for a date range
url = "https://api.frankfurter.app/2024-01-01..2024-06-30?from=USD"
response = requests.get(url)
data = response.json()

# Insert raw JSON into Snowflake
cursor = conn.cursor()
cursor.execute(
    "INSERT INTO RAW_EXCHANGE_RATES (RAW_DATA) SELECT PARSE_JSON(%s)",
    (json.dumps(data),)
)

print("Data loaded successfully!")
cursor.close()
conn.close()
```

---

### Phase 3: dbt Transformations

#### 3.1 Staging Model
The staging model flattens the nested JSON into individual rows.

**Key Snowflake concepts used:**
- `VARIANT` data type for semi-structured data
- `:` syntax to access JSON keys
- `LATERAL FLATTEN()` to unnest arrays/objects
- `::` for type casting

**models/staging/stg_exchange_rates.sql:**
```sql
SELECT 
    date_key.key::DATE AS rate_date,
    raw.RAW_DATA:base::VARCHAR AS base_currency,
    currency.key::VARCHAR AS target_currency,
    currency.value::FLOAT AS rate
FROM DE_EXCHANGE_RATES.MAIN.RAW_EXCHANGE_RATES raw,
    LATERAL FLATTEN(input => raw.RAW_DATA:rates) date_key,
    LATERAL FLATTEN(input => date_key.value) currency
```

**How FLATTEN works:**
1. First FLATTEN breaks out each date from the `rates` object
2. Second FLATTEN breaks out each currency within each date
3. Result: one row per date per currency

#### 3.2 Incremental Fact Model
The fact model implements incremental loading - only processing new data on subsequent runs.

**models/marts/fct_exchange_rates.sql:**
```sql
{{
    config(
        materialized='incremental',
        unique_key=['rate_date', 'base_currency', 'target_currency']
    )
}}

SELECT 
    rate_date,
    base_currency,
    target_currency,
    rate
FROM {{ ref('stg_exchange_rates') }}

{% if is_incremental() %}
    WHERE rate_date > (SELECT MAX(rate_date) FROM {{ this }})
{% endif %}
```

**How incremental works:**
- **First run:** `is_incremental()` is FALSE → loads ALL data
- **Subsequent runs:** `is_incremental()` is TRUE → only loads rows where `rate_date` > max existing date
- **unique_key:** Prevents duplicates; updates existing rows if same key found

---

### Phase 4: Testing & Documentation

#### 4.1 Schema Tests
Added data quality tests in `schema.yml` files:

**models/staging/schema.yml:**
```yaml
version: 2

models:
  - name: stg_exchange_rates
    description: "Flattened exchange rate data from Frankfurter API"
    columns:
      - name: rate_date
        description: "Date of the exchange rate"
        tests:
          - not_null
      - name: base_currency
        description: "Source currency (USD)"
        tests:
          - not_null
      - name: target_currency
        description: "Target currency code"
        tests:
          - not_null
      - name: rate
        description: "Exchange rate value"
        tests:
          - not_null
```

#### 4.2 Run Tests
```bash
dbt test
```

#### 4.3 Generate Documentation
```bash
dbt docs generate
dbt docs serve
```

---

## How to Run This Project

### Prerequisites
- Snowflake account (free trial works)
- Python 3.x installed
- dbt Core installed (`pip3 install dbt-snowflake`)

### Steps

1. **Clone the repository**
```bash
git clone https://github.com/arunchandar99/de-exchange-rates-pipeline.git
cd de-exchange-rates-pipeline
```

2. **Configure Snowflake connection**
Edit `~/.dbt/profiles.yml` with your Snowflake credentials

3. **Create Snowflake objects**
```sql
CREATE OR REPLACE DATABASE DE_EXCHANGE_RATES;
CREATE OR REPLACE SCHEMA DE_EXCHANGE_RATES.MAIN;
CREATE OR REPLACE TABLE DE_EXCHANGE_RATES.MAIN.RAW_EXCHANGE_RATES (
    RAW_DATA VARIANT,
    LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

4. **Install Python dependencies**
```bash
pip3 install requests snowflake-connector-python
```

5. **Load initial data**
```bash
python3 load_raw_data.py
```

6. **Run dbt models**
```bash
dbt run
```

7. **Run tests**
```bash
dbt test
```

8. **View documentation**
```bash
dbt docs generate
dbt docs serve
```

---

## Testing Incremental Logic

To verify incremental loading works:

1. Check current row count:
```sql
SELECT COUNT(*), MAX(rate_date) FROM DE_EXCHANGE_RATES.MAIN.FCT_EXCHANGE_RATES;
```

2. Modify `load_raw_data.py` to fetch new date range (e.g., July 2024)

3. Run the pipeline again:
```bash
python3 load_raw_data.py
dbt run
```

4. Verify only new rows were added:
```sql
SELECT COUNT(*), MAX(rate_date) FROM DE_EXCHANGE_RATES.MAIN.FCT_EXCHANGE_RATES;
```

---

## Key Learnings

1. **Snowflake VARIANT type** - Stores semi-structured data (JSON) natively
2. **LATERAL FLATTEN** - Unnests nested JSON structures into rows
3. **dbt incremental models** - Process only new data, improving efficiency
4. **dbt ref()** - Creates dependencies between models automatically
5. **dbt testing** - Ensures data quality with built-in tests
6. **dbt documentation** - Auto-generates project docs with lineage

---

## Future Enhancements

- [ ] Add CI/CD with GitHub Actions
- [ ] Schedule daily runs with Airflow or cron
- [ ] Add more currencies and base currency options
- [ ] Create dimensional models (dim_currency)
- [ ] Add data quality alerts

---

## Author

Arun Chandar

---

## Resources

- [Frankfurter API Documentation](https://www.frankfurter.app/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Snowflake FLATTEN Documentation](https://docs.snowflake.com/en/sql-reference/functions/flatten)
