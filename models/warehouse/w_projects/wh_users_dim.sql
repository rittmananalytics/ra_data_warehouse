{% if not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='user_pk',
        alias='users_dim'
    )
}}
{% endif %}

WITH users AS
  (
    SELECT {{ dbt_utils.star(from=ref('int_users')) }}
    FROM {{ ref('int_users') }}
)
select GENERATE_UUID() as user_pk,
       u.*
from users u
