{{config(enabled = target.type == 'snowflake')}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}


{% if var("stg_hubspot_crm_etl") == 'fivetran' %}

with source AS (
  SELECT *
  FROM {{ var('stg_hubspot_crm_fivetran_deals_table') }}
),
hubspot_deal_company AS (
  SELECT *
  FROM {{ var('stg_hubspot_crm_fivetran_companies_table') }}
),
hubspot_deal_pipelines_source AS (
  SELECT *
  FROM  {{ var('stg_hubspot_crm_fivetran_deal_pipelines_table') }}
)
,
hubspot_deal_property_history AS (
  SELECT *
  FROM  {{ var('stg_hubspot_crm_fivetran_property_history_table') }}
)
,
hubspot_deal_stages AS (
  SELECT *
  FROM  {{ var('stg_hubspot_crm_fivetran_pipeline_stages_table') }}
),
hubspot_deal_owners AS (
  SELECT *
  FROM {{ var('stg_hubspot_crm_fivetran_deal_owners_table') }}
),
renamed AS (
  SELECT
      deal_id AS deal_id,
      property_dealname     AS deal_name,
      property_dealtype     AS deal_type,
      property_description  AS deal_description,
      deal_pipeline_stage_id AS deal_pipeline_stage_id,
      deal_pipeline_id        AS deal_pipeline_id,
      is_deleted             AS deal_is_deleted,
      property_amount        AS deal_amount,
      owner_id AS deal_owner_id,
      property_amount_in_home_currency    AS deal_amount_local_currency,
      property_closed_lost_reason         AS deal_closed_lost_reason,
      property_closedate                  AS deal_closed_date,
      property_createdate                 AS deal_created_date,
      property_hs_lastmodifieddate        AS deal_last_modified_date
      FROM
  source
),
joined AS (
    SELECT
    d.deal_id,
    CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',CAST(a.company_id AS string)) AS company_id,
    d.* except (deal_id),
    timestamp_millis(CAST(h.value AS int)) AS deal_pipeline_stage_ts,
    p.pipeline_label,
    p.pipeline_display_order,
    s.pipeline_stage_label,
    s.pipeline_stage_display_order,
    s.pipeline_stage_close_probability_pct,
    s.pipeline_stage_closed_won,
    u.owner_full_name,
    u.owner_email
    FROM renamed d
    left outer join hubspot_deal_company a on d.deal_id = a.deal_id
    left outer join hubspot_deal_property_history h on d.deal_id = h.deal_id and h.name = CONCAT('hs_date_entered_',d.deal_pipeline_stage_id)
    join hubspot_deal_stages s on d.deal_pipeline_stage_id = s.pipeline_stage_id
    join hubspot_deal_pipelines_source p on s.pipeline_id = p.pipeline_id
    left outer join hubspot_deal_owners u on CAST(d.deal_owner_id AS int) = u.owner_id
)

{% elif var("stg_hubspot_crm_etl") == 'stitch' %}

with source AS (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_deals_table'),unique_column='dealid') }}

),
hubspot_deal_pipelines_source AS (
  SELECT *
  FROM
  {{ ref('stg_hubspot_crm_pipelines') }}
)
,
hubspot_deal_stages AS (
  SELECT *
  FROM  {{ ref('stg_hubspot_crm_pipeline_stages') }}
),
hubspot_deal_owners AS (
  SELECT *
  FROM {{ ref('stg_hubspot_crm_owners') }}
),
renamed AS (
  SELECT
        CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',dealid::STRING) AS deal_id,
        CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',associations:associatedcompanyids:value::STRING) AS company_id,
        property_dealname:value::STRING                                     AS deal_name,
        case when property_dealtype:value::STRING = 'newbusiness' then 'New Business'
             when property_dealtype:value::STRING = 'existingbusiness' then 'Existing Client'
             else 'Existing Client' end AS deal_type,
        property_description:value::STRING                                  AS deal_description,
        property_createdate:value::TIMESTAMP                                   AS deal_created_ts,
        property_delivery_schedule_date:value::TIMESTAMP AS delivery_schedule_ts,
        property_delivery_start_date:value::TIMESTAMP AS delivery_start_date_ts,
        property_closedate:value::TIMESTAMP                                    AS deal_closed_ts,
        property_hs_lastmodifieddate:value::TIMESTAMP                          AS deal_last_modified_ts,
        property_dealstage:value::STRING                                    AS deal_pipeline_stage_id,
        property_dealstage:timestamp::timestamp                                AS deal_pipeline_stage_ts,
        property_end_date:value::timestamp         AS deal_end_ts,
        property_hs_sales_email_last_replied:value::STRING                  AS deal_sales_email_last_replied,
        property_engagements_last_meeting_booked:value::TIMESTAMP              AS deal_last_meeting_booked_date,
        property_hs_deal_stage_probability:value::FLOAT                    AS deal_stage_probability_pct,
        property_pipeline:value::STRING                                     AS deal_pipeline_id,
        property_hubspot_team_id:value::STRING                              AS hubspot_team_id,
        property_hubspot_owner_id:value::STRING                             AS deal_owner_id,
        property_hs_created_by_user_id:value::INT                        AS created_by_user_id,
        CAST(null AS boolean)                                      AS deal_is_deleted,
        property_deal_currency_code:value::STRING                           AS deal_currency_code,
        property_source:value::STRING                                       AS deal_source,
        property_hs_analytics_source:value::STRING                          AS hs_analytics_source,
        property_hs_analytics_source_data_1:value::STRING                   AS hs_analytics_source_data_1,
        property_hs_analytics_source_data_2:value::STRING                   AS hs_analytics_source_data_2,
        property_amount:value::STRING                                       AS deal_amount,
        property_hs_projected_amount_in_home_currency:value::INT         AS projected_home_currency_amount,
        property_amount_in_home_currency:value::INT                      AS projected_local_currency_amount,
        property_hs_tcv:value::INT                                       AS deal_total_contract_amount,
        property_hs_acv:value::INT                                       AS deal_annual_contract_amount,
        property_hs_arr:value::INT                                       AS deal_annual_recurring_revenue_amount,
        property_hs_closed_amount:value::INT                             AS deal_closed_amount_value,
        property_hs_closed_amount_in_home_currency:value::INT            AS hs_closed_amount_in_home_currency,
        property_days_to_close:value::INT                                AS deal_days_to_close,
        property_closed_lost_reason:value::STRING                           AS deal_closed_lost_reason,
        property_harvest_project_id:value::STRING                           AS deal_harvest_project_id,
        property_number_of_sprints:value::FLOAT                            AS deal_number_of_sprints,
        property_deal_components:value::STRING                              AS deal_components,
        case when property_deal_components:value::STRING  like '%Services%' then true else false end AS is_services_deal,
        case when property_deal_components:value::STRING  like '%Managed Services%' then true else false end AS is_managed_services_deal,
        case when property_deal_components:value::STRING  like '%License Referral%' then true else false end AS is_license_referral_deal,
        case when property_deal_components:value::STRING  like '%Training%' then true else false end AS is_training_deal,
        case when property_deal_components:value::STRING  like '%Looker%' then true else false end AS is_looker_skill_requirement,
        case when property_products_in_solution:value::STRING like '%Segment%' then true else false end AS is_segment_skill_requirement,
        case when property_products_in_solution:value::STRING like '%dbt%' then true else false end AS is_dbt_skill_requirement,
        case when property_products_in_solution:value::STRING like '%Stitch%' then true else false end AS is_stitch_skill_requirement,
        case when property_products_in_solution:value::STRING like '%GCP%' then true else false end AS is_gcp_skill_requirement,
        case when property_products_in_solution:value::STRING like '%Snowflake%' then true else false end AS is_snowflake_skill_requirement,
        case when property_products_in_solution:value::STRING like '%Qubit%' then true else false end AS is_qubit_skill_requirement,
        case when property_products_in_solution:value::STRING like '%Fivetran%' then true else false end AS is_fivetran_skill_requirement,
        property_pricing_model:value::STRING                                AS deal_pricing_model,
        property_partner_referral:value::STRING                             AS deal_partner_referral,
        property_sprint_type:value::STRING                                  AS deal_sprint_type,
        property_license_referral_harvest_project_code:value::STRING        AS deal_license_referral_harvest_project_code,
        property_jira_project_code:value::STRING                            AS deal_jira_project_code,
        property_assigned_consultant:value::STRING                          AS deal_assigned_consultant,
        property_products_in_solution:value::STRING                         AS deal_products_in_solution,
        property_hs_manual_forecast_category:value::STRING AS manual_forecast_category,
        property_hs_forecast_probability:value::FLOAT AS forecast_probability,
        property_hs_merged_object_ids:value::STRING AS merged_object_ids,
        property_hs_predicted_amount:value::STRING AS predicted_amount
      FROM
      source
),
joined AS (
    SELECT
    d.*,
    p.pipeline_label,
    p.pipeline_display_order,
    s.pipeline_stage_label,
    s.pipeline_stage_display_order,
    s.pipeline_stage_close_probability_pct,
    s.pipeline_stage_closed_won,
    u.owner_full_name,
    u.owner_email
    FROM renamed d
    join hubspot_deal_stages s on d.deal_pipeline_stage_id = s.pipeline_stage_id
    join hubspot_deal_pipelines_source p on s.pipeline_id = p.pipeline_id
    left outer join hubspot_deal_owners u on CAST(d.deal_owner_id AS int) = u.owner_id
)
{% endif %}
SELECT * FROM joined

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
