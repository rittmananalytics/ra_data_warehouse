{%- macro filter_segment_relation(relation) -%}

SELECT
  * EXCEPT (loaded_at, max_loaded_at)
FROM
  (
    SELECT
      *,
      MAX(loaded_at) OVER (PARTITION BY id ORDER BY loaded_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_loaded_at
    FROM
      {{ relation }}
  )
WHERE
  loaded_at = max_loaded_at
{%- endmacro -%}
