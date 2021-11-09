{% if var("dev_warehouse_git_sources") %}

with issue_merged as (
    select *
    from {{ ref('stg_github_issue_merged') }}
),
issue as (
    select *
    from {{ ref('stg_github_issue') }}
    where is_pull_request is true
),
joined as (
    select
        im.commit_sha,
        im.issue_id,
        im.merged_at,
        im.user_id,
        i.repository_id
    from issue_merged im 
    left join issue i
        on im.issue_id = i.issue_id
)
select *
from joined

{% endif %}
