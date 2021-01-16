{% if not var("enable_compliance_watchlist_source") %}
  {{
      config(
          enabled=false
      )
  }}
{% endif %}

with source as (

  select * from {{ source('analytics_compliance','s_compliance_watchlist' ) }}

)

select * from source