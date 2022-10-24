{% macro staging_models_automation(package, source_schema, source_database, tables) %}

{% set package = ""~ package ~"" %}
{% set source_schema = ""~ source_schema ~"" %}
{% set source_database = ""~ source_database ~"" %}

{% set zsh_command = "source dbt_modules/fivetran_utils/columns_setup.sh '../dbt_"""~ package ~"""_source' stg_"""~ package ~""" """~ source_database ~""" """~ source_schema ~""" " %}

{% for t in tables %}
    {% if t != tables[-1] %}
        {% set help_command = zsh_command + t + " && \n" %}

    {% else %}
        {% set help_command = zsh_command + t %}

    {% endif %}
    {{ log(help_command, info=True) }}

{% endfor %}

{% endmacro %} 
