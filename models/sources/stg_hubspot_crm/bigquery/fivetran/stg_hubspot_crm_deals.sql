{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}



with source AS (
  SELECT *
  FROM {{ source('fivetran_hubspot_crm','deals') }}
),
hubspot_deal_company AS (
  SELECT *
  FROM {{ source('fivetran_hubspot_crm','companies') }}
),
hubspot_deal_pipelines_source AS (
  SELECT *
  FROM  {{ source('fivetran_hubspot_crm','deal_pipelines') }}
)
,
hubspot_deal_property_history AS (
  SELECT *
  FROM  {{ source('fivetran_hubspot_crm','property_history') }}
)
,
hubspot_deal_stages AS (
  SELECT *
  FROM  {{ source('fivetran_hubspot_crm','pipeline_stages') }}
),
hubspot_deal_owners AS (
  SELECT *
  FROM {{ source('fivetran_hubspot_crm','deal_owners') }}
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
    timestamp_millis(safe_CAST(h.value AS int64)) AS deal_pipeline_stage_ts,
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
    left outer join hubspot_deal_owners u on safe_CAST(d.deal_owner_id AS int64) = u.owner_id
)
SELECT * FROM joined

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
