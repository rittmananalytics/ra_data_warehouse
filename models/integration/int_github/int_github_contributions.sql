{% if var("dev_warehouse_git_sources") %}

with commit as (
    select *
    from {{ ref('int_github_commit') }}
),
pull_request as (
    select *
    from {{ ref('int_github_pull_request') }}
),
pull_request_review as (
    select *
    from {{ ref('int_github_pull_request_review') }}
),
pull_request_merged as (
    select *
    from {{ ref('int_github_pull_request_merged') }}
),
release as (
    select *
    from {{ ref('stg_github_release') }}
),
renamed_unioned as (
    select
        'commit' as contribution_type,
        user_id,
        repository_id,
        author_date as contributed_at
    from commit

union all

    select
        'pr_raised' as contribution_type,
        user_id,
        repository_id,
        created_at as contributed_at
    from pull_request

union all

    select
        'pr_' || state as contribution_type,
        user_id,
        repository_id,
        submitted_at as contributed_at
    from pull_request_review

union all

    select
        'pr_merged' as contribution_type,
        user_id,
        repository_id,
        merged_at as contributed_at
    from pull_request_merged

union all

    select
        'release' as contribution_type,
        user_id,
        repository_id,
        created_at as contributed_at
    from release

)
select *
from  renamed_unioned

{% endif %}
