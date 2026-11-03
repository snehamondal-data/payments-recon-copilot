-- Every bank line with no reference, with how many charges it could belong to.
-- Exactly one candidate: matched on amount. None: orphan. More than one: ambiguous.

with candidates as (
    select
        line_id,
        count(*) as candidate_count,
        min(charge_id) as first_candidate_charge_id,
        {{ dbt.listagg('charge_id', "','", 'order by charge_id') }} as candidate_charge_ids
    from {{ ref('int_unreferenced_line_candidates') }}
    group by line_id
)

select
    lines.line_id,
    lines.value_date,
    lines.direction,
    lines.description,
    lines.net_amount_minor,
    coalesce(candidates.candidate_count, 0) as candidate_count,
    case when candidates.candidate_count = 1 then candidates.first_candidate_charge_id end as matched_charge_id,
    candidates.candidate_charge_ids
from {{ ref('stg_bank_settlement_lines') }} as lines
left join candidates
    on lines.line_id = candidates.line_id
where lines.reference is null
