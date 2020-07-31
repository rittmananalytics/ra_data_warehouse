{% macro generate_model_name(alias) %}
  {{ config(schema = 'profiles') }}

{% set names_query %}
    WITH source AS (

      SELECT
        *,
        MAX(load_ts) OVER (
          PARTITION BY guid
          ORDER BY
            load_ts RANGE BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
        ) AS max_load_ts
      FROM
        {{ target.schema ~ '_logs.audit_dbt_results_test' }}
    ),
    distinct_names AS (
      SELECT
        node,
        object
      FROM
        source
      WHERE
        load_ts = max_load_ts
      GROUP BY
        1,2
    )
    SELECT
      node
    FROM
      distinct_names
    WHERE
      object in '{{alias}}'
{% endset %}

{% set results = run_query(names_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values()  %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ log(results_list, info=True) }}
{{ return(results_list) }}

{% endmacro %}
