{% if var("dev_warehouse_git_sources") %}

with commit as (
    select *
    from {{ ref('stg_github_commit') }}
),
user_email as (
    select *
    from {{ ref('stg_github_user_email') }}
),
joined as (
    select
        commit_sha,
        ue.user_id,
        author_date,
        author_email,
        author_name,
        committer_date,
        committer_email,
        committer_name,
        message,
        repository_id
    from commit c
    left join user_email ue
        on c.author_email = ue.email
)
select *
from joined

{% endif %}
