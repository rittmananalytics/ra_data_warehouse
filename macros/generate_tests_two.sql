{% macro generate_tests_two(profile_name) %}

{{ config(schema='profiles') }}


{% set catalog_names = dbt_utils.get_query_results_as_dict("select distinct table_catalog from " ~  profile_name ) %}
{% set schema_names = dbt_utils.get_query_results_as_dict("select distinct table_schema from " ~  profile_name ) %}

{#-- Creates new Yaml File for each Catalog. #}
{% for table_catalog, catalog in catalog_names.items() %}
  {#-- Creates new Yaml File for each Schema. #}
  {% for table_schema, schema  in schema_names.items() %}

    {% set model_yaml=[] %}
    {# {% do model_yaml.append('# schema for tests in ' ~  catalog[0] ~ ' catalog and automatically generated tests for all tables in ' ~ schema[0] ~ ' schema.') %} #}
    {% do model_yaml.append("version: 2") %}
    {% do model_yaml.append('') %}

    {#-- gets the table names in the given schema & catalog. #}
    {% set table_names = dbt_utils.get_query_results_as_dict("select distinct table_name from " ~ profile_name ~ " where table_catalog = '" ~ catalog[0]  ~ "' and table_schema =  '" ~ schema[0] ~ "'") %}


    {% do model_yaml.append('models:') %}

    {#--iterate over each table. #}
    {% for table_name, table in table_names.items() %}
      {% do model_yaml.append('  - name: ' ~ table[0] | lower) %}

      {% set columns = dbt_utils.get_query_results_as_dict("select column_name, logical_or(is_recommended_not_nullable_column) as null_test, logical_or(is_recommended_unique_column) as unique_test from " ~  profile_name  ~ "  where table_catalog = '" ~ catalog[0]  ~ "'  and table_schema = '" ~ schema[0] ~ "' and table_name ='" ~  table[0] ~ "' and is_recommended_not_nullable_column = true OR is_recommended_unique_column = true group by 1 order by 1") %}

      {#--iterate over each column. #}
      {% for column_name in columns.column_name %}
        {% do model_yaml.append('      - name: ' ~ column_name| lower ) %}
        {% do model_yaml.append('        description: ""') %}

        {# assumes column_name & null_test tuples stay in order relative to each other? #}
        {% do model_yaml.append('        tests:') %}
        {% if columns.null_test[loop.index0] is sameas true  %}
        {% do model_yaml.append('          - not_null' ) %}
        {% endif %}

        {% if columns.unique_test[loop.index0] is sameas true %}
        {% do model_yaml.append('          - unique') %}
        {% endif %}

        {% do model_yaml.append('') %}

      {% endfor %}

    {% endfor %}

    {% if execute %}

    {% set joined = model_yaml | join ('\n') %}
    {{ log(joined, info=True) }}
    {# {% do return(joined) %} #}

    {% endif %}

  {% endfor %}

{% endfor %}

{% endmacro %}
