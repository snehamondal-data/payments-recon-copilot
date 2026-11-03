-- Every processor charge lands in exactly one of: matched, pending, an exception of its own,
-- or (as a candidate) inside an ambiguous bank-line exception.

with placements as (
    select charge_id from {{ ref('recon_matches') }}
    union all
    select charge_id from {{ ref('recon_pending') }}
    union all
    select charge_id from {{ ref('recon_exceptions') }} where charge_id is not null
    union all
    select charge_id from {{ ref('int_charge_recon') }} where ambiguous_line_id is not null
),

counted as (
    select charges.charge_id, count(placements.charge_id) as placements
    from {{ ref('int_processor_charges') }} as charges
    left join placements
        on charges.charge_id = placements.charge_id
    group by charges.charge_id
)

select * from counted where placements <> 1
