{% if not var("enable_hubspot_crm_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("etl") == 'fivetran' %}

with source as (
  select *
  from {{ var('fivetran_deal_table') }}
),
hubspot_deal_company as (
  select *
  from {{ var('fivetran_company_table') }}
),
hubspot_deal_pipelines_source as (
  select *
  from  {{ var('fivetran_deal_pipeline_table') }}
)
,
hubspot_deal_property_history as (
  select *
  from  {{ var('fivetran_property_history_table') }}
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
    concat('{{ var('id-prefix') }}',cast(a.company_id as string)) as company_id,
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

{% elif var("etl") == 'stitch' %}

with source as (
  {{ filter_stitch_table(var('stitch_deals_table'),'dealid') }}

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
      concat('{{ var('id-prefix') }}',cast(associations.associatedcompanyids[offset(off)] as string)) as company_id,
      properties.dealname.value     as deal_name,
      properties.dealtype.value     as deal_type,
      properties.description.value  as deal_description,
      properties.dealstage.value as deal_pipeline_stage_id,
      properties.dealstage.timestamp as deal_pipeline_stage_ts,
      properties.pipeline.value     as deal_pipeline_id,
      cast (null as boolean)        as deal_is_deleted,
      properties.amount.value        as deal_amount,
      properties.hubspot_owner_id.value as deal_owner_id,
      properties.amount_in_home_currency.value    as deal_amount_local_currency,
      properties.closed_lost_reason.value         as deal_closed_lost_reason,
      properties.closedate.value                  as deal_closed_date,
      properties.createdate.value                 as deal_created_date,
      properties.hs_lastmodifieddate.value        as deal_last_modified_date
      FROM
      source,
                unnest(associations.associatedcompanyids) with offset off
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
