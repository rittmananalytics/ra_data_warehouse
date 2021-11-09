{{config(enabled = target.type == 'bigquery')}}
{% if var("dev_warehouse_git_sources") %}
{% if 'github' in var("dev_warehouse_git_sources") %}
{% if var("stg_github_etl") == 'fivetran' %}

with source as (
    select *
    from {{ source('fivetran_github_sources','pull_request') }}
),
renamed as (
    select
        cast(id as numeric) as pull_request_id,
        cast(base_sha as string) as base_sha,
        cast(base_repo_id as numeric) as base_repo_id,
        cast(base_user_id as numeric) as base_user_id,
        cast(head_sha as string) as head_sha,
        cast(head_repo_id as numeric) as head_repo_id,
        cast(head_user_id as numeric) as head_user_id,
        cast(issue_id as numeric) as issue_id,
        cast(merge_commit_sha as string) as merge_commit_sha,
        cast(base_label as string) as base_label,
        cast(base_ref as string) as base_ref,
        cast(draft as boolean) as is_draft,
        cast(head_label as string) as head_label,
        cast(head_ref as string) as head_ref
    from source
)
select *
from renamed

{% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}