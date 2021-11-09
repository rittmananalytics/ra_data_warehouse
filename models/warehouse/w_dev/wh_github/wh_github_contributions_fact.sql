{% if var("dev_warehouse_git_sources") %}

with contributions as (
    select *
    from {{ ref('int_github_contributions') }}
),
final as (
    select
        contribution_type,
        user_id,
        repository_id,
        contributed_at
    from contributions 
)
select *
from final

{% endif %}
