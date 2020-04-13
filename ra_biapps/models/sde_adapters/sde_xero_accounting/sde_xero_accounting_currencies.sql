{% if not var("enable_xero_accounting") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with currencies as
  (
    SELECT
      *
    FROM (
      SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY code ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ source('xero_accounting', 'currencies') }})
    WHERE
      max_sdc_batched_at = _sdc_batched_at
    )
select code currency_code,
       description as currency_name, from currencies
