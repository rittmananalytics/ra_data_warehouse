{% if not var("enable_hubspot_crm_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("stg_hubspot_crm_etl") == 'fivetran' %}

with source as (
  select *
  from {{ var('stg_hubspot_crm_fivetran_schema') }}.{{ var('stg_hubspot_crm_fivetran_deal_table') }}
),
hubspot_deal_company as (
  select *
  from {{ var('stg_hubspot_crm_fivetran_schema') }}.{{ var('stg_hubspot_crm_fivetran_company_table') }}
),
hubspot_deal_pipelines_source as (
  select *
  from  {{ var('stg_hubspot_crm_fivetran_schema') }}.{{ var('stg_hubspot_crm_fivetran_deal_pipeline_table') }}
)
,
hubspot_deal_property_history as (
  select *
  from  {{ var('stg_hubspot_crm_fivetran_schema') }}.{{ var('stg_hubspot_crm_fivetran_property_history_table') }}
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
      deal_id,
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
    timestamp_millis(safe_cast(h.value as int64)) as deal_pipeline_stage_ts,
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
    left outer join hubspot_deal_owners u on safe_cast(d.deal_owner_id as int64) = u.owner_id
)

{% elif var("stg_hubspot_crm_etl") == 'stitch' %}

with source as (
  {{ filter_stitch_table(var('stg_hubspot_crm_stitch_schema'),var('stg_hubspot_crm_stitch_deals_table'),'dealid') }}

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
      dealid as deal_id,
      concat('{{ var('stg_hubspot_crm_id-prefix') }}',cast(associatedcompanyids.value as string)) as company_id,
      property_dealname.value                                     as deal_name,
      case when property_dealtype.value = 'newbusiness' then 'New Business'
           when property_dealtype.value = 'existingbusiness' then 'Existing Client'
           else property_dealtype.value end as deal_type,
      property_description.value                                  as deal_description,
      property_createdate.value                                   as deal_created_ts,
      TIMESTAMP_MILLIS(safe_cast(property_delivery_schedule_date.value  as int64)) as delivery_schedule_ts,
      TIMESTAMP_MILLIS(safe_cast(property_delivery_start_date.value     as int64)) as delivery_start_date_ts,
      property_closedate.value                                    as deal_closed_ts,
      property_hs_lastmodifieddate.value                          as deal_last_modified_ts,
      property_dealstage.value                                    as deal_pipeline_stage_id,
      property_dealstage.timestamp                                as deal_pipeline_stage_ts,
      TIMESTAMP_MILLIS(safe_cast(property_end_date.value as int64))         as deal_end_ts,
      property_hs_sales_email_last_replied.value                  as deal_sales_email_last_replied,
      property_engagements_last_meeting_booked.value              as deal_last_meeting_booked_date,
      property_hs_deal_stage_probability.value                    as deal_stage_probability_pct,
      property_pipeline.value                                     as deal_pipeline_id,
      property_hubspot_team_id.value                              as hubspot_team_id,
      property_hubspot_owner_id.value                             as deal_owner_id,
      property_hs_created_by_user_id.value                        as created_by_user_id,
      cast (null as boolean)                                      as deal_is_deleted,
      property_deal_currency_code.value                           as deal_currency_code,
      property_source.value                                       as deal_source,
      property_hs_analytics_source.value                          as hs_analytics_source,
      property_hs_analytics_source_data_1.value                   as hs_analytics_source_data_1,
      property_hs_analytics_source_data_2.value                   as hs_analytics_source_data_2,
      property_amount.value                                       as deal_amount,
      property_hs_projected_amount_in_home_currency.value         as projected_home_currency_amount,
      property_amount_in_home_currency.value                      as projected_local_currency_amount,
      property_hs_tcv.value                                       as deal_total_contract_amount,
      property_hs_acv.value                                       as deal_annual_contract_amount,
      property_hs_arr.value                                       as deal_annual_recurring_revenue_amount,
      property_hs_closed_amount.value                             as deal_closed_amount_value,
      property_hs_closed_amount_in_home_currency.value            as hs_closed_amount_in_home_currency,
      property_days_to_close.value                                as deal_days_to_close,
      property_closed_lost_reason.value                           as deal_closed_lost_reason,
      property_harvest_project_id.value                           as deal_harvest_project_id,
      property_number_of_sprints.value                            as deal_number_of_sprints,
      property_deal_components.value                              as deal_components,
      case when property_deal_components.value  like '%Services%' then true else false end as is_services_deal,
      case when property_deal_components.value  like '%Managed Services%' then true else false end as is_managed_services_deal,
      case when property_deal_components.value  like '%License Referral%' then true else false end as is_license_referral_deal,
      case when property_deal_components.value  like '%Training%' then true else false end as is_training_deal,
      case when property_deal_components.value  like '%Looker%' then true else false end as is_looker_skill_requirement,
      case when property_products_in_solution.value like '%Segment%' then true else false end as is_segment_skill_requirement,
      case when property_products_in_solution.value like '%dbt%' then true else false end as is_dbt_skill_requirement,
      case when property_products_in_solution.value like '%Stitch%' then true else false end as is_stitch_skill_requirement,
      case when property_products_in_solution.value like '%GCP%' then true else false end as is_gcp_skill_requirement,
      case when property_products_in_solution.value like '%Snowflake%' then true else false end as is_snowflake_skill_requirement,
      case when property_products_in_solution.value like '%Qubit%' then true else false end as is_qubit_skill_requirement,
      case when property_products_in_solution.value like '%Fivetran%' then true else false end as is_fivetran_skill_requirement,
      property_pricing_model.value                                as deal_pricing_model,
      property_partner_referral.value                             as deal_partner_referral,
      property_sprint_type.value                                  as deal_sprint_type,
      property_license_referral_harvest_project_code.value        as deal_license_referral_harvest_project_code,
      property_jira_project_code.value                            as deal_jira_project_code,
      property_assigned_consultant.value                          as deal_assigned_consultant,
      property_products_in_solution.value                         as deal_products_in_solution
      FROM
      source,
                unnest(associations.associatedcompanyids) associatedcompanyids
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
    left outer join hubspot_deal_owners u on safe_cast(d.deal_owner_id as int64) = u.owner_id
)
{% endif %}
select * from joined
