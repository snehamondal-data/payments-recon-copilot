-- Bank lines that cannot be tied to exactly one charge.

with charges as (
    select charge_id from {{ ref('int_processor_charges') }}
),

-- A reference that names no known charge.
unknown_reference as (
    select
        lines.line_id,
        lines.value_date,
        lines.net_amount_minor,
        'orphan_bank_deposit' as reason_code,
        cast(null as {{ dbt.type_string() }}) as related_charge_ids
    from {{ ref('stg_bank_settlement_lines') }} as lines
    left join charges
        on lines.reference = charges.charge_id
    where lines.reference is not null
        and charges.charge_id is null
),

-- No reference, and either no candidate charge or several.
unreferenced as (
    select
        line_id,
        value_date,
        net_amount_minor,
        case when candidate_count = 0 then 'orphan_bank_deposit' else 'ambiguous' end as reason_code,
        candidate_charge_ids as related_charge_ids
    from {{ ref('int_unreferenced_lines') }}
    where candidate_count <> 1
)

select * from unknown_reference
union all
select * from unreferenced
