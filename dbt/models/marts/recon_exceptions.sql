-- The exceptions queue: one row per thing that doesn't tie out, keyed by the item it is about.
-- Charges absorbed into an ambiguous bank line appear once, on that line, in related_charge_ids.
-- Charges still inside their settlement window are never here (see recon_pending).

with calendar as (
    select * from {{ ref('int_business_calendar') }}
),

charge_exceptions as (
    select
        {{ dbt.concat(["'charge:'", 'charge_id']) }} as exception_id,
        'charge' as item_type,
        charge_id,
        cast(null as {{ dbt.type_string() }}) as bank_line_id,
        reason_code,
        detected_problems,
        amount_minor,
        charge_date_utc as item_date,
        age_business_days,
        window_status,
        cast(null as {{ dbt.type_string() }}) as related_charge_ids
    from {{ ref('int_charge_recon') }}
    where window_status = 'exception'
        and ambiguous_line_id is null
),

line_exceptions as (
    select
        {{ dbt.concat(["'bank_line:'", 'lines.line_id']) }} as exception_id,
        'bank_line' as item_type,
        cast(null as {{ dbt.type_string() }}) as charge_id,
        lines.line_id as bank_line_id,
        lines.reason_code,
        lines.reason_code as detected_problems,
        lines.net_amount_minor as amount_minor,
        lines.value_date as item_date,
        as_of_day.business_day_index - line_day.business_day_index as age_business_days,
        'exception' as window_status,
        lines.related_charge_ids
    from {{ ref('int_bank_line_exceptions') }} as lines
    inner join calendar as line_day
        on lines.value_date = line_day.date_day
    inner join calendar as as_of_day
        on as_of_day.date_day = {{ as_of_date() }}
)

select *, {{ as_of_date() }} as as_of_date from charge_exceptions
union all
select *, {{ as_of_date() }} as as_of_date from line_exceptions
