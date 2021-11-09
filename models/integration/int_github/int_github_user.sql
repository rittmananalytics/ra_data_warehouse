{% if var("dev_warehouse_git_sources") %}

with user as (
    select *
    from {{ ref('stg_github_user') }}
),
user_flags as (
    select *
    from {{ ref('stg_ra_github_user_flags') }}
),
joined as (
    select
        u.user_id,
        u.bio,
        u.blog,
        u.company,
        u.created_at,
        u.username,
        u.name,
        u.type,
        u.location,
        u.updated_at,
        u.is_site_admin,
        u.is_hireable,
        uf.is_rittman,
        uf.is_contractor
    from user u
    left join user_flags uf
        on u.username = uf.github_username
)
select *
from joined

{% endif %}
