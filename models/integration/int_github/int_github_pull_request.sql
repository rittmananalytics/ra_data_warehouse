{% if var("dev_warehouse_git_sources") %}

with pull_request as (
    select *
    from {{ ref('stg_github_pull_request') }}
),
issue as (
    select *
    from {{ ref('stg_github_issue') }}
),
joined as (
    select
        pr.pull_request_id,
        pr.base_sha,
        pr.base_repo_id,
        pr.base_user_id,
        pr.head_sha,
        pr.head_repo_id,
        pr.head_user_id,
        pr.issue_id,
        pr.merge_commit_sha,
        pr.base_label,
        pr.base_ref,
        pr.is_draft,
        pr.head_label,
        pr.head_ref,
        i.created_at,
        i.repository_id,
        i.user_id
    from pull_request pr
    left join issue i
        on pr.issue_id = i.issue_id
)
select *
from joined

{% endif %}
