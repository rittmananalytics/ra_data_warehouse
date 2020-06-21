{%- macro filter_segment_table(schema_name, source_table) -%}

SELECT
  * EXCEPT (loaded_at, max_loaded_at)
FROM
  (
    SELECT
      *,
      MAX(loaded_at) OVER (PARTITION BY id ORDER BY loaded_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_loaded_at
    FROM
      {{ target.database}}.{{ schema_name }}.{{ source_table }}
  )
WHERE
  loaded_at = max_loaded_at
{%- endmacro -%}
