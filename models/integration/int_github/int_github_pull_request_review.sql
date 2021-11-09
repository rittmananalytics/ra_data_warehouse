{% if var("dev_warehouse_git_sources") %}

with pr_review as (
    select *
    from {{ ref('stg_github_pull_request_review') }}
),
commit as (
    select *
    from {{ ref('stg_github_commit') }}
),
joined as (
    select
        prr.review_id,
        prr.body,
        prr.commit_sha,
        prr.pull_request_id,
        prr.state,
        prr.submitted_at,
        prr.user_id,
        c.repository_id
    from pr_review prr
    left join commit c 
        on prr.commit_sha = c.commit_sha
)
select *
from joined

{% endif %}
