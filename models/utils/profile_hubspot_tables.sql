{% set tables = dbt_utils.get_relations_by_prefix('stitch_hubspot', '') %}
{% for table in tables %}
select column_stats.*,
       column_metadata.* except (table_catalog,
                                 table_schema,
                                 table_name,
                                 column_name)
 from (
  select * from
(
  WITH
    `table` AS (SELECT * FROM {{ table }} ),
    table_as_json AS (SELECT REGEXP_REPLACE(TO_JSON_STRING(t), r'^{|}$', '') AS ROW FROM `table` AS t ),
    pairs AS (SELECT REPLACE(column_name, '"', '') AS column_name, IF (SAFE_CAST(column_value AS STRING)='null',NULL, column_value) AS column_value
              FROM table_as_json,UNNEST(SPLIT(ROW, ',"')) AS z,UNNEST([SPLIT(z, ':')[SAFE_OFFSET(0)]]) AS column_name,UNNEST([SPLIT(z, ':')[SAFE_OFFSET(1)]]) AS column_value ),
    profile AS (
    SELECT
      split('{{ table }}','`.`')[safe_offset(1)] as table_catalog,
      split('{{ table }}','`.`')[safe_offset(1)] as table_schema,
      replace(split('{{ table }}','`.`')[safe_offset(2)],'`','') as table_name,
      column_name,
      COUNT(*) AS table_rows,
      COUNT(DISTINCT column_value) AS _distinct_values,
      safe_divide(COUNT(DISTINCT column_value),
      COUNT(*))*100 AS pct_unique,
      COUNTIF(column_value IS NULL) AS _nulls,
      COUNTIF(column_value IS NOT NULL) AS _non_nulls,
      COUNTIF(column_value IS NULL) / COUNT(*) AS pct_null,
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
  select * from profile)
{%- if not loop.last %}
    UNION ALL
{%- endif %}
{% endfor %}
) column_stats
left join
(
  SELECT
 * EXCEPT(is_generated, generation_expression, is_stored, is_updatable)
FROM
 analytics.INFORMATION_SCHEMA.COLUMNS
) column_metadata
on  column_stats.table_catalog = column_metadata.table_catalog
and column_stats.table_schema = column_metadata.table_schema
and column_stats.table_name = column_metadata.table_name
and column_stats.column_name = column_metadata.column_name
