{%- macro filter_stitch_source(schema_name, source_name, table_name, unique_column) -%}

SELECT *
  EXCEPT (ROW_NUMBER)
FROM (
	SELECT *,
  ROW_NUMBER() OVER (PARTITION BY {{ unique_column }} ORDER BY loaded_at DESC) ROW_NUMBER FROM {{ source(source_name,table_name) }}
)
  WHERE ROW_NUMBER = 1
{%- endmacro -%}
