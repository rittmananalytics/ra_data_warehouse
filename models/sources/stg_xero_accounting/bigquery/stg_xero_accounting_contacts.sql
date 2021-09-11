{{ config(
  enabled = target.type == 'bigquery'
) }}

{% if var("crm_warehouse_contact_sources") %}
  {% if 'xero_accounting' in var("crm_warehouse_contact_sources") %}
    {% if var("stg_xero_accounting_etl") == 'fivetran' %}
      WITH source AS (

        SELECT
          *
        FROM
          {{ source(
            'fivetran_xero_accounting',
            'contact'
          ) }}
      ),
      addresses AS (
        SELECT
          contact_id,
          address_type,
          address_line_1,
          address_line_2,
          address_line_3,
          address_line_4,
          city,
          region,
          country,
          postal_code
        FROM
          {{ source(
            'fivetran_xero_accounting',
            'contact_address'
          ) }}
      ),
      renamed AS (
        SELECT
          CONCAT(
            '{{ var(' stg_xero_accounting_id - prefix ') }}',
            contacts.contact_id
          ) AS contact_id,
          contacts.first_name AS contact_first_name,
          contacts.last_name AS contact_last_name,
          CAST(NULL AS {{ dbt_utils.type_string() }}) AS contact_job_title,
          COALESCE(CONCAT(contacts.first_name, ' ', contacts.last_name), contacts.email_address) AS contact_name,
          contacts.email_address AS contact_email,
          CAST(NULL AS {{ dbt_utils.type_string() }}) AS company_phone,
          {{ fivetran_utils.string_agg(
            'addresses.address_line_1',
            ','
          ) }} AS contact_address,
          {{ fivetran_utils.string_agg(
            'addresses.city',
            ','
          ) }} AS contact_city,
          {{ fivetran_utils.string_agg(
            'addresses..region',
            ','
          ) }} AS contact_state,
          {{ fivetran_utils.string_agg(
            'addresses.country',
            ','
          ) }} AS contact_country,
          {{ fivetran_utils.string_agg(
            'addresses.postal_code',
            ','
          ) }} AS contact_postcode_zip,
          CAST(NULL AS {{ dbt_utils.type_string() }}) AS contact_company,
          CAST(NULL AS {{ dbt_utils.type_string() }}) AS contact_website,
          CAST(NULL AS {{ dbt_utils.type_string() }}) AS contact_company_id,
          CAST(NULL AS {{ dbt_utils.type_string() }}) AS contact_owner_id,
          contacts.contact_status AS contact_lifecycle_stage,
          CAST(NULL AS {{ dbt_utils.type_boolean() }}) AS contact_is_contractor,
          CAST(NULL AS {{ dbt_utils.type_boolean() }}) AS contact_is_staff,
          CAST(NULL AS {{ dbt_utils.type_int() }}) AS contact_weekly_capacity,
          CAST(NULL AS {{ dbt_utils.type_int() }}) AS contact_default_hourly_rate,
          CAST(NULL AS {{ dbt_utils.type_int() }}) AS contact_cost_rate,
          FALSE AS contact_is_active,
          CAST(NULL AS {{ dbt_utils.type_timestamp() }}) AS contact_created_date,
          CAST(
            contacts.updated_date_utc AS {{ dbt_utils.type_timestamp() }}
          )
        FROM
          source AS contacts
          LEFT OUTER JOIN addresses AS addresses
          ON contacts.contact_id = addresses.contact_id
          AND addresses.address_type = 'STREET'
        WHERE
          CONCAT(
            contacts.first_name,
            ' ',
            contacts.last_name
          ) IS NOT NULL
        GROUP BY
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          13,
          14,
          15,
          16,
          17,
          18,
          19,
          20,
          21,
          22,
          23,
          24,
          25
      )
    {% endif %}
  SELECT
    *
  FROM
    renamed
  {% else %}
    {{ config(
      enabled = false
    ) }}
  {% endif %}
{% else %}
  {{ config(
    enabled = false
  ) }}
{% endif %}
