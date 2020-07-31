{%- macro generate_tests(table_schema) -%}

{{ config(schema='profiles') }}

{% set tables = dbt_utils.get_relations_by_prefix(table_schema, '') %}

{% for table in tables %}

SELECT
    'version: 2' as version,
    ARRAY
    (SELECT AS STRUCT
    table_name as name,
    model_description as description,
    ARRAY
    (SELECT AS STRUCT
        column_name as name,
        column_description as description
     from
     (
      SELECT
           table_catalog as project_name,
           table_schema as schema_name,
           table_name,
           column_name,
           description as column_description
         from {{ table_schema }}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
         where column_name = field_path
          AND table_name= split(replace('{{ table }}','`',''),'.' )[safe_offset(2)]
       )
  ) AS columns

FROM (
  SELECT
    table_catalog as project_name,
    table_schema as schema_name,
    table_name,
    option_value as model_description
  FROM
    {{ table_schema }}.INFORMATION_SCHEMA.TABLE_OPTIONS
  where table_name= split(replace('{{ table }}','`',''),'.' )[safe_offset(2)]
  AND option_name = 'description'
)
) AS models
{%- if not loop.last %}
    UNION ALL
{%- endif %}
{% endfor %}


{%- endmacro -%}
