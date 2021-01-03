{{config(enabled = target.type == 'bigquery')}}
{% if not var("enable_gcp_billing_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (
    SELECT
      *
    FROM
      {{ source('gcp_billing', 's_gcp_billing_export') }}
  ),
 renamed as
 (
  SELECT
        billing_account_id,
        project.id AS project_id,
        location.location as billing_data_location,
        location.country  as billing_data_country,
        location.region,
        location.zone,
        SUM(cost) AS total_cost,
        SUM(coalesce(usage.amount,0)) AS total_usage_amount,
        usage.unit,
        SUM(coalesce(usage.amount_in_pricing_units,0)) AS total_amount_in_pricing_units,
        usage.pricing_unit,
        currency,
        AVG(coalesce(currency_conversion_rate,0)) AS avg_currency_conversion_rate,
        invoice.month as billing_month,
        service.id AS service_id,
        service.description
        FROM
          source
        GROUP BY
          1,2,3,4,5,6,9,11,12,14,15,16)
SELECT
 *
FROM
 renamed
