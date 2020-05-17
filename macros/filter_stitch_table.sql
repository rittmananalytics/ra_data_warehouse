{%- macro filter_stitch_table(source_table, unique_column) -%}

SELECT
  * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
FROM
  (
    SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY {{ unique_column }} ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM
      {{ target.database}}.{{ source_table }}
  )
WHERE
  _sdc_batched_at = max_sdc_batched_at

{%- endmacro -%}
