select
*
from {{ source('main', 'raw_exchange_rates') }}

