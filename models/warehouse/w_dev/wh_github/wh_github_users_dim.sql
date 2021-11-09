{% if var("dev_warehouse_git_sources") %}

with users as (
    select *
    from {{ ref('int_github_user') }}
),
final as (
    select
        user_id,
        company,
        created_at,
        username,
        name,
        type,
        is_rittman,
        is_contractor
    from users 
)
select *
from final

{% endif %}
