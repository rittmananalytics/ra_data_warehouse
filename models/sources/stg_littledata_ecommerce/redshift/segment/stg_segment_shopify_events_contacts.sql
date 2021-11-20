{{config(enabled = target.type == 'redshift')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'segment_shopify_events' in var("crm_warehouse_contact_sources") %}

with source AS (

  SELECT u.*,
  n.sample_orders,
  n.non_sample_orders
  from
    {{ var('stg_segment_shopify_events_segment_users_table') }} u
  left join
    (SELECT json_extract_path_text(replace(replace(context_external_ids,'[',''),']',''), 'id') AS user_id,
     sum(case when REGEXP_COUNT ( products, 'Sample')<1 then 1 else 0 end) AS non_sample_orders,
     sum(case when REGEXP_COUNT ( products, 'Sample')>0 then 1 else 0 end) AS sample_orders
     FROM production_shopify_by_littledata.order_completed
     group by 1) n
on u.id = n.user_id

),
renamed AS (
    SELECT

       CAST(id AS {{ dbt_utils.type_string() }}) AS contact_id,
       first_name AS first_name,
       last_name AS last_name,
       CONCAT(CONCAT(first_name,' '),last_name) AS contact_name,
        CAST(null AS {{ dbt_utils.type_string() }}) contact_job_title,
       email AS contact_email,
       phone AS contact_phone,
       address_street contact_address,
       address_city contact_city,
       address_state contact_state,
       address_country AS contact_country,
       address_postal_code contact_postcode_zip,
        CAST(null AS {{ dbt_utils.type_string() }}) contact_company,
        CAST(null AS {{ dbt_utils.type_string() }}) contact_website,
        CAST(null AS {{ dbt_utils.type_string() }}) AS contact_company_id,
        CAST(null AS {{ dbt_utils.type_string() }}) AS contact_owner_id,
        CAST(null AS {{ dbt_utils.type_string() }}) AS contact_lifecycle_stage,
       CAST(null AS {{ dbt_utils.type_boolean() }}) AS contact_is_staff,
       state='enabled'                          AS contact_is_active,
       coalesce((REGEXP_COUNT ( tags, 'PRO,')>0 or REGEXP_COUNT ( tags, 'PRO_25,')>0 or REGEXP_COUNT ( tags, 'PRO_20,')>0
        or REGEXP_COUNT ( tags, 'PRO_15,')>0 or REGEXP_COUNT ( tags, 'PRO_FACEBOOK')),false) AS contact_is_pro,
        marketing_opt_in AS contact_is_marketing_opt_in,
        customer_lifetime_value AS contact_lifetime_value,
        purchase_count AS contact_purchase_count,
        verified_email AS contact_has_verified_email,
        accepts_marketing AS contact_accepts_marketing,
        non_sample_orders AS contact_non_sample_orders,
        sample_orders AS contact_sample_orders,
       created_at AS contact_created_date,
        CAST(null AS {{ dbt_utils.type_timestamp() }}) AS contact_last_modified_date
    FROM source
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
