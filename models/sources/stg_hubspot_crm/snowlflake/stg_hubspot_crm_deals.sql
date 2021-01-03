{{config(enabled = target.type == 'snowflake')}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}


{% if var("stg_hubspot_crm_etl") == 'fivetran' %}

with source as (
  select *
  from {{ var('stg_hubspot_crm_fivetran_deals_table') }}
),
hubspot_deal_company as (
  select *
  from {{ var('stg_hubspot_crm_fivetran_companies_table') }}
),
hubspot_deal_pipelines_source as (
  select *
  from  {{ var('stg_hubspot_crm_fivetran_deal_pipelines_table') }}
)
,
hubspot_deal_property_history as (
  select *
  from  {{ var('stg_hubspot_crm_fivetran_property_history_table') }}
)
,
hubspot_deal_stages as (
  select *
  from  {{ var('stg_hubspot_crm_fivetran_pipeline_stages_table') }}
),
hubspot_deal_owners as (
  SELECT *
  FROM {{ var('stg_hubspot_crm_fivetran_deal_owners_table') }}
),
renamed as (
  SELECT
      deal_id as deal_id,
      property_dealname     as deal_name,
      property_dealtype     as deal_type,
      property_description  as deal_description,
      deal_pipeline_stage_id as deal_pipeline_stage_id,
      deal_pipeline_id        as deal_pipeline_id,
      is_deleted             as deal_is_deleted,
      property_amount        as deal_amount,
      owner_id as deal_owner_id,
      property_amount_in_home_currency    as deal_amount_local_currency,
      property_closed_lost_reason         as deal_closed_lost_reason,
      property_closedate                  as deal_closed_date,
      property_createdate                 as deal_created_date,
      property_hs_lastmodifieddate        as deal_last_modified_date
      FROM
  source
),
joined as (
    select
    d.deal_id,
    concat('{{ var('stg_hubspot_crm_id-prefix') }}',cast(a.company_id as string)) as company_id,
    d.* except (deal_id),
    timestamp_millis(cast(h.value as int)) as deal_pipeline_stage_ts,
    p.pipeline_label,
    p.pipeline_display_order,
    s.pipeline_stage_label,
    s.pipeline_stage_display_order,
    s.pipeline_stage_close_probability_pct,
    s.pipeline_stage_closed_won,
    u.owner_full_name,
    u.owner_email
    from renamed d
    left outer join hubspot_deal_company a on d.deal_id = a.deal_id
    left outer join hubspot_deal_property_history h on d.deal_id = h.deal_id and h.name = concat('hs_date_entered_',d.deal_pipeline_stage_id)
    join hubspot_deal_stages s on d.deal_pipeline_stage_id = s.pipeline_stage_id
    join hubspot_deal_pipelines_source p on s.pipeline_id = p.pipeline_id
    left outer join hubspot_deal_owners u on cast(d.deal_owner_id as int) = u.owner_id
)

{% elif var("stg_hubspot_crm_etl") == 'stitch' %}

with source as (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_deals_table'),unique_column='dealid') }}

),
hubspot_deal_pipelines_source as (
  SELECT *
  FROM
  {{ ref('stg_hubspot_crm_pipelines') }}
)
,
hubspot_deal_stages as (
  select *
  from  {{ ref('stg_hubspot_crm_pipeline_stages') }}
),
hubspot_deal_owners as (
  SELECT *
  FROM {{ ref('stg_hubspot_crm_owners') }}
),
renamed as (
  SELECT
        concat('{{ var('stg_hubspot_crm_id-prefix') }}',dealid::STRING) as deal_id,
        concat('{{ var('stg_hubspot_crm_id-prefix') }}',associations:associatedcompanyids:value::STRING) as company_id,
        property_dealname:value::STRING                                     as deal_name,
        case when property_dealtype:value::STRING = 'newbusiness' then 'New Business'
             when property_dealtype:value::STRING = 'existingbusiness' then 'Existing Client'
             else 'Existing Client' end as deal_type,
        property_description:value::STRING                                  as deal_description,
        property_createdate:value::TIMESTAMP                                   as deal_created_ts,
        property_delivery_schedule_date:value::TIMESTAMP as delivery_schedule_ts,
        property_delivery_start_date:value::TIMESTAMP as delivery_start_date_ts,
        property_closedate:value::TIMESTAMP                                    as deal_closed_ts,
        property_hs_lastmodifieddate:value::TIMESTAMP                          as deal_last_modified_ts,
        property_dealstage:value::STRING                                    as deal_pipeline_stage_id,
        property_dealstage:timestamp::timestamp                                as deal_pipeline_stage_ts,
        property_end_date:value::timestamp         as deal_end_ts,
        property_hs_sales_email_last_replied:value::STRING                  as deal_sales_email_last_replied,
        property_engagements_last_meeting_booked:value::TIMESTAMP              as deal_last_meeting_booked_date,
        property_hs_deal_stage_probability:value::FLOAT                    as deal_stage_probability_pct,
        property_pipeline:value::STRING                                     as deal_pipeline_id,
        property_hubspot_team_id:value::STRING                              as hubspot_team_id,
        property_hubspot_owner_id:value::STRING                             as deal_owner_id,
        property_hs_created_by_user_id:value::INT                        as created_by_user_id,
        cast (null as boolean)                                      as deal_is_deleted,
        property_deal_currency_code:value::STRING                           as deal_currency_code,
        property_source:value::STRING                                       as deal_source,
        property_hs_analytics_source:value::STRING                          as hs_analytics_source,
        property_hs_analytics_source_data_1:value::STRING                   as hs_analytics_source_data_1,
        property_hs_analytics_source_data_2:value::STRING                   as hs_analytics_source_data_2,
        property_amount:value::STRING                                       as deal_amount,
        property_hs_projected_amount_in_home_currency:value::INT         as projected_home_currency_amount,
        property_amount_in_home_currency:value::INT                      as projected_local_currency_amount,
        property_hs_tcv:value::INT                                       as deal_total_contract_amount,
        property_hs_acv:value::INT                                       as deal_annual_contract_amount,
        property_hs_arr:value::INT                                       as deal_annual_recurring_revenue_amount,
        property_hs_closed_amount:value::INT                             as deal_closed_amount_value,
        property_hs_closed_amount_in_home_currency:value::INT            as hs_closed_amount_in_home_currency,
        property_days_to_close:value::INT                                as deal_days_to_close,
        property_closed_lost_reason:value::STRING                           as deal_closed_lost_reason,
        property_harvest_project_id:value::STRING                           as deal_harvest_project_id,
        property_number_of_sprints:value::FLOAT                            as deal_number_of_sprints,
        property_deal_components:value::STRING                              as deal_components,
        case when property_deal_components:value::STRING  like '%Services%' then true else false end as is_services_deal,
        case when property_deal_components:value::STRING  like '%Managed Services%' then true else false end as is_managed_services_deal,
        case when property_deal_components:value::STRING  like '%License Referral%' then true else false end as is_license_referral_deal,
        case when property_deal_components:value::STRING  like '%Training%' then true else false end as is_training_deal,
        case when property_deal_components:value::STRING  like '%Looker%' then true else false end as is_looker_skill_requirement,
        case when property_products_in_solution:value::STRING like '%Segment%' then true else false end as is_segment_skill_requirement,
        case when property_products_in_solution:value::STRING like '%dbt%' then true else false end as is_dbt_skill_requirement,
        case when property_products_in_solution:value::STRING like '%Stitch%' then true else false end as is_stitch_skill_requirement,
        case when property_products_in_solution:value::STRING like '%GCP%' then true else false end as is_gcp_skill_requirement,
        case when property_products_in_solution:value::STRING like '%Snowflake%' then true else false end as is_snowflake_skill_requirement,
        case when property_products_in_solution:value::STRING like '%Qubit%' then true else false end as is_qubit_skill_requirement,
        case when property_products_in_solution:value::STRING like '%Fivetran%' then true else false end as is_fivetran_skill_requirement,
        property_pricing_model:value::STRING                                as deal_pricing_model,
        property_partner_referral:value::STRING                             as deal_partner_referral,
        property_sprint_type:value::STRING                                  as deal_sprint_type,
        property_license_referral_harvest_project_code:value::STRING        as deal_license_referral_harvest_project_code,
        property_jira_project_code:value::STRING                            as deal_jira_project_code,
        property_assigned_consultant:value::STRING                          as deal_assigned_consultant,
        property_products_in_solution:value::STRING                         as deal_products_in_solution,
        property_hs_manual_forecast_category:value::STRING as manual_forecast_category,
        property_hs_forecast_probability:value::FLOAT as forecast_probability,
        property_hs_merged_object_ids:value::STRING as merged_object_ids,
        property_hs_predicted_amount:value::STRING as predicted_amount
      FROM
      source
),
joined as (
    select
    d.*,
    p.pipeline_label,
    p.pipeline_display_order,
    s.pipeline_stage_label,
    s.pipeline_stage_display_order,
    s.pipeline_stage_close_probability_pct,
    s.pipeline_stage_closed_won,
    u.owner_full_name,
    u.owner_email
    from renamed d
    join hubspot_deal_stages s on d.deal_pipeline_stage_id = s.pipeline_stage_id
    join hubspot_deal_pipelines_source p on s.pipeline_id = p.pipeline_id
    left outer join hubspot_deal_owners u on cast(d.deal_owner_id as int) = u.owner_id
)
{% endif %}
select * from joined

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
