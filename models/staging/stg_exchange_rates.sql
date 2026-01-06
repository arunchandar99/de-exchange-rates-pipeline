SELECT 
    date_key.key::DATE AS rate_date,
    raw.RAW_DATA:base::VARCHAR AS base_currency,
    currency.key::VARCHAR AS target_currency,
    currency.value::FLOAT AS rate
FROM DE_EXCHANGE_RATES.MAIN.RAW_EXCHANGE_RATES raw,
    LATERAL FLATTEN(input => raw.RAW_DATA:rates) date_key,
    LATERAL FLATTEN(input => date_key.value) currency

