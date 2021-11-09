{{config(enabled = target.type == 'bigquery')}}
{% if var("dev_warehouse_git_sources") %}
{% if 'github' in var("dev_warehouse_git_sources") %}
{% if var("stg_github_etl") == 'fivetran' %}

with source as (
    select *
    from {{ source('fivetran_github_sources','user_email') }}
),
renamed as (
    select
        cast(email as string) as email,
        cast(user_id as numeric) as user_id,
        cast(name as string) as name
    from source
)
select *
from renamed

{% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
