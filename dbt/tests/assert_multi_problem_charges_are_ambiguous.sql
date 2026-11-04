-- A charge with more than one detected problem must be ambiguous (so it is escalated), and must list them all.
select charge_id, reason_code, problem_count, detected_problems
from {{ ref('int_charge_recon') }}
where problem_count > 1
    and (reason_code <> 'ambiguous' or detected_problems not like '%,%')
