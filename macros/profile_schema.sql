{%- macro profile_schema(table_schema) -%}

{{ config(schema='profiles') }}

{% set not_null_profile_threshold_pct = ".9" %}
{% set unique_profile_threshold_pct = ".9" %}

{% set tables = dbt_utils.get_relations_by_prefix(table_schema, '') %}

SELECT column_stats.table_catalog,
       column_stats.table_schema,
       column_stats.table_name,
       column_stats.column_name,
       case when column_metadata.is_nullable = 'YES' then false else true end as is_not_nullable_column,
       case when column_stats.pct_not_null > {{ not_null_profile_threshold_pct }} then true else false end as is_recommended_not_nullable_column,

       column_stats._nulls as count_nulls,
       column_stats._non_nulls as count_not_nulls,
       column_stats.pct_not_null as pct_not_null,
       column_stats.table_rows,
       column_stats.count_distinct_values,
       column_stats.pct_unique,
       case when column_stats.pct_unique >= {{ unique_profile_threshold_pct }} then true else false end as is_recommended_unique_column,

       column_metadata.* EXCEPT (table_catalog,
                       table_schema,
                       table_name,
                       column_name,
                       is_nullable),
       column_stats.* EXCEPT (table_catalog,
                              table_schema,
                              table_name,
                              column_name,
                              _nulls,
                              _non_nulls,
                              pct_not_null,
                              table_rows,
                              pct_unique,
                              count_distinct_values)
FROM
(
{% for table in tables %}
  SELECT *
  FROM
(
  WITH
    `table` AS (SELECT * FROM {{ table }} ),
    table_as_json AS (SELECT REGEXP_REPLACE(TO_JSON_STRING(t), r'^{|}$', '') AS ROW FROM `table` AS t ),
    pairs AS (SELECT REPLACE(column_name, '"', '') AS column_name, IF (SAFE_CAST(column_value AS STRING)='null',NULL, column_value) AS column_value
              FROM table_as_json,UNNEST(SPLIT(ROW, ',"')) AS z,UNNEST([SPLIT(z, ':')[SAFE_OFFSET(0)]]) AS column_name,UNNEST([SPLIT(z, ':')[SAFE_OFFSET(1)]]) AS column_value ),
    profile AS (
    SELECT
      split(replace('{{ table }}','`',''),'.' )[safe_offset(0)] as table_catalog,
      split(replace('{{ table }}','`',''),'.' )[safe_offset(1)] as table_schema,
      split(replace('{{ table }}','`',''),'.' )[safe_offset(2)] as table_name,
      column_name,
      COUNT(*) AS table_rows,
      COUNT(DISTINCT column_value) AS count_distinct_values,
      safe_divide(COUNT(DISTINCT column_value),COUNT(*)) AS pct_unique,
      COUNTIF(column_value IS NULL) AS _nulls,
      COUNTIF(column_value IS NOT NULL) AS _non_nulls,
      COUNTIF(column_value IS NOT NULL) / COUNT(*) AS pct_not_null,
      min(column_value) as _min_value,
      max(column_value) as _max_value,
      avg(SAFE_CAST(column_value AS numeric)) as _avg_value,
      APPROX_TOP_COUNT(column_value, 1)[OFFSET(0)] AS _most_frequent_value,
      MIN(LENGTH(SAFE_CAST(column_value AS STRING))) AS _min_length,
      MAX(LENGTH(SAFE_CAST(column_value AS STRING))) AS _max_length,
      ROUND(AVG(LENGTH(SAFE_CAST(column_value AS STRING)))) AS _avr_length
    FROM
      pairs
    WHERE
      column_name <> ''
      AND column_name NOT LIKE '%-%'
    GROUP BY
      column_name
    ORDER BY
      column_name)
  SELECT
    *
  FROM
    profile)
{%- if not loop.last %}
    UNION ALL
{%- endif %}
{% endfor %}
) column_stats
LEFT OUTER JOIN
(
  SELECT
    * EXCEPT
      (is_generated,
       generation_expression,
       is_stored,
       is_updatable)
  FROM
    {{ table_schema }}.INFORMATION_SCHEMA.COLUMNS
) column_metadata
ON  column_stats.table_catalog = column_metadata.table_catalog
AND column_stats.table_schema = column_metadata.table_schema
AND column_stats.table_name = column_metadata.table_name
AND column_stats.column_name = column_metadata.column_name

{%- endmacro -%}
