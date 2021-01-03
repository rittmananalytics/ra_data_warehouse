{%- macro filter_segment_relation(relation) -%}

SELECT
  * EXCEPT (uuid_ts, max_uuid_ts)
FROM
  (
    SELECT
      *,
      MAX(uuid_ts) OVER (PARTITION BY id ORDER BY uuid_ts RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_loaded_at
    FROM
      {{ relation }}
  )
WHERE
  uuid_ts = max_uuid_ts
{%- endmacro -%}
