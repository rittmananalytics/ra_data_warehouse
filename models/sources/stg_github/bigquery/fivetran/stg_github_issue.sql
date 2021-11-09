{{config(enabled = target.type == 'bigquery')}}
{% if var("dev_warehouse_git_sources") %}
{% if 'github' in var("dev_warehouse_git_sources") %}
{% if var("stg_github_etl") == 'fivetran' %}

with source as (
    select *
    from {{ source('fivetran_github_sources','issue') }}
),
renamed as (
    select
        cast(id as numeric) as issue_id,
        cast(milestone_id as numeric) as milestone_id,
        cast(repository_id as numeric) as repository_id,
        cast(user_id as numeric) as user_id,
        cast(body as string) as body,
        cast(closed_at as datetime) as closed_at,
        cast(created_at as datetime) as created_at,
        cast(locked as boolean) as is_locked,
        cast(number as numeric) as number,
        cast(state as string) as state,
        cast(title as string) as title,
        cast(updated_at as datetime) as updated_at,
        cast(pull_request as boolean) as is_pull_request
    from source
)
select *
from renamed

{% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
