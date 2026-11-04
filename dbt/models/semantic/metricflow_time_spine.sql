-- The daily time spine MetricFlow requires, taken from the business-day calendar.
select cast(date_day as date) as date_day
from {{ ref('int_business_calendar') }}
