
with currencies as
  (
    SELECT
      *
    FROM (
      SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY code ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
      FROM
        {{ source('xero_accounting', 'currencies') }})
    WHERE
      latest_sdc_batched_at = _sdc_batched_at
    )
select 'xero_accounting'       source,
        code currency_code,
       description as currency_name, from currencies
