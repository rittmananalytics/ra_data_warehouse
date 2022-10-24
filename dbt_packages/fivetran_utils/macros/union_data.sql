{% macro union_data(table_identifier, database_variable, schema_variable, default_database, default_schema, default_variable, union_schema_variable='union_schemas', union_database_variable='union_databases') -%}

{{ adapter.dispatch('union_data', 'fivetran_utils') (
    table_identifier, 
    database_variable, 
    schema_variable, 
    default_database, 
    default_schema, 
    default_variable,
    union_schema_variable,
    union_database_variable
    ) }}

{%- endmacro %}

{% macro default__union_data(
    table_identifier, 
    database_variable, 
    schema_variable, 
    default_database, 
    default_schema, 
    default_variable,
    union_schema_variable,
    union_database_variable
    ) %}

{% if var(union_schema_variable, none) %}

    {% set relations = [] %}
    
    {% if var(union_schema_variable) is string %}
    {% set trimmed = var(union_schema_variable)|trim('[')|trim(']') %}
    {% set schemas = trimmed.split(',')|map('trim'," ")|map('trim','"')|map('trim',"'") %}
    {% else %}
    {% set schemas = var(union_schema_variable) %}
    {% endif %}

    {% for schema in var(union_schema_variable) %}

    {% set relation=adapter.get_relation(
        database=var(database_variable, default_database),
        schema=schema,
        identifier=table_identifier
    ) -%}
    
    {% set relation_exists=relation is not none %}

    {% if relation_exists %}

    {% do relations.append(relation) %}
    
    {% endif %}

    {% endfor %}

    {{ dbt_utils.union_relations(relations) }}

{% elif var(union_database_variable, none) %}

    {% set relations = [] %}

    {% for database in var(union_database_variable) %}

    {% set relation=adapter.get_relation(
        database=database,
        schema=var(schema_variable, default_schema),
        identifier=table_identifier
    ) -%}

    {% set relation_exists=relation is not none %}

    {% if relation_exists %}

    {% do relations.append(relation) %}
    
    {% endif %}

    {% endfor %}

    {{ dbt_utils.union_relations(relations) }}

{% else %}

    select * 
    from {{ var(default_variable) }}

{% endif %}

{% endmacro %}
