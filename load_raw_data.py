# ============================================
# IMPORTS - Loading external libraries we need
# ============================================

# 'requests' lets us make HTTP calls to APIs (like visiting a URL)
import requests

# 'snowflake.connector' lets Python talk to Snowflake
import snowflake.connector

# 'json' helps us work with JSON data (convert between text and Python objects)
import json


# ============================================
# SNOWFLAKE CONNECTION - Setting up the link to your database
# ============================================

# This creates a connection to Snowflake using your credentials
# Think of it like logging into Snowflake, but from Python
conn = snowflake.connector.connect(
    user='arunchandar',              # Your Snowflake username
    password='**********',     # Your Snowflake password
    account='ACCNYTE-WXC26408',      # Your Snowflake account identifier
    warehouse='COMPUTE_WH',          # The warehouse to use for compute
    database='DE_EXCHANGE_RATES',    # The database we created
    schema='MAIN'                    # The schema we created
)


# ============================================
# API CALL - Fetching exchange rate data
# ============================================

# This is the API URL - asking for USD exchange rates from Jan 1 to June 30, 2024
url = "https://api.frankfurter.app/2024-07-01..2024-07-31?from=USD"

# requests.get() visits that URL and gets the response (like your browser does)
response = requests.get(url)

# .json() converts the response text into a Python dictionary we can work with
data = response.json()


# ============================================
# LOAD INTO SNOWFLAKE - Inserting the data
# ============================================

# A cursor is like a pointer that lets us execute SQL commands
cursor = conn.cursor()

# This runs an INSERT statement in Snowflake:
# - json.dumps(data) converts our Python dictionary back to a JSON string
# - PARSE_JSON() tells Snowflake to treat it as JSON (VARIANT type)
# - %s is a placeholder that gets replaced with our data (safe way to insert)
cursor.execute(
    "INSERT INTO RAW_EXCHANGE_RATES (RAW_DATA) SELECT PARSE_JSON(%s)",
    (json.dumps(data),)
)

# Print a success message so we know it worked
print("Data loaded successfully!")

# Close the cursor and connection (cleanup - like logging out)
cursor.close()
conn.close()
