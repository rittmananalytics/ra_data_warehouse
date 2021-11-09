{% if var("dev_warehouse_git_sources") %}

with repos as (
    select *
    from {{ ref('stg_github_repository') }}
),
final as (
    select
        repository_id,
        repository_name,
        owner_user_id,
        full_name,
        created_at,
        description,
        default_branch,
        is_fork,
        is_archived,
        is_private
    from repos 
)
select *
from final

{% endif %}
