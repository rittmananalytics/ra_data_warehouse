{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contacts_sources") %}
{% if 'intercom_messaging' in var("crm_warehouse_contacts_sources") %}

WITH source AS (
      {{ filter_stitch_relation(relation=source('stitch_intercom_messaging', 'contacts'),unique_column='id') }}
  ),
renamed AS (
  SELECT
    CONCAT('{{ var('stg_intercom_messaging_id-prefix') }}',id) AS contact_id,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_first_name,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_last_name,
    custom_attributes.job_title AS job_title,
    email  AS contact_email,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_phone,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_address,
    location_data.city_name AS contact_city,
    location_data.region_name AS contact_state,
    location_data.country_name AS contact_country,
    location_data.postal_code contact_postcode_zip,
    CAST(null AS {{ dbt_utils.type_string() }})  contact_company,
    CAST(null AS {{ dbt_utils.type_string() }})  contact_website,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_company_id,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_owner_id,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_lifecycle_stage,
    CAST(null AS {{ dbt_utils.type_boolean() }})         AS contact_is_contractor,
    CAST(null AS {{ dbt_utils.type_boolean() }}) AS contact_is_staff,
     CAST(null AS {{ dbt_utils.type_int() }})           AS contact_weekly_capacity,
     CAST(null AS {{ dbt_utils.type_int() }})           AS contact_default_hourly_rate,
     CAST(null AS {{ dbt_utils.type_int() }})           AS contact_cost_rate,
    false                          AS contact_is_active,
    created_at AS contact_created_date,
    updated_at AS contact_last_modified_date
  FROM
    source)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
