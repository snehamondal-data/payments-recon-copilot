select
    rate_date,
    currency,
    rate_to_usd
from {{ source('ref', 'fx_rates') }}
