{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}



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
select * from joined

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
