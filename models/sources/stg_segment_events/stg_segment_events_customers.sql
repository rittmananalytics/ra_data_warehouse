{% if not var("enable_segment_dashboard_events_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
with source as (with source as (

    select * from {{ var('stg_segment_events_segment_users_table') }}

),),
renamed as (
   select concat('stg_segment_events_id-prefix',id) as customer_id,
   email as customer_email,
   cast(null as string) as customer_description,
   username as customer_alternative_id,
   traits_plan as customer_plan,
  cast(null as string) as customer_source,
  user_type as customer_type,
  industry as customer_industry,
  cast(null as string) as customer_currency,
  enterprise as customer_is_enterprise,
  cast(null as boolean) as customer_is_delinquent,
  cast(null as boolean) as customer_is_deleted,
  min(received_at) over (partition by id) as customer_created_date,
  max(received_at) over (partition by id) as customer_last_modified_date
FROM
 source
)
select * from renamed
