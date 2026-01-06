-- ============================================
-- CONFIGURATION BLOCK
-- This tells dbt how to build this model
-- 'incremental' means: don't rebuild the whole table every time, just add new rows
-- 'unique_key' defines what makes a row unique - prevents duplicates
-- ============================================
{{
    config(
        materialized='incremental',
        unique_key=['rate_date', 'base_currency', 'target_currency']
    )
}}

-- ============================================
-- MAIN QUERY
-- This pulls the flattened exchange rate data from our staging model
-- ============================================

SELECT 
    rate_date,           -- the date of the exchange rate
    base_currency,       -- the source currency (USD in our case)
    target_currency,     -- the currency we're converting to (EUR, GBP, etc.)
    rate                 -- the exchange rate value
    
-- ref() is a dbt function that references another model
-- dbt automatically figures out the full table path
-- it also builds stg_exchange_rates first if needed (dependency management)
FROM {{ ref('stg_exchange_rates') }}

-- ============================================
-- INCREMENTAL LOGIC
-- This block only runs when the table already exists (not on first run)
-- is_incremental() returns TRUE if the table exists and we're not doing --full-refresh
-- {{ this }} refers to THIS table (fct_exchange_rates)
-- ============================================

{% if is_incremental() %}
    WHERE rate_date > (SELECT MAX(rate_date) FROM {{ this }})
{% endif %}