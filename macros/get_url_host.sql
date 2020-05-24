{% macro get_url_host(field) -%}

{%- set parsed = 
    split_part(
        split_part(
            dbt_utils.replace(
                dbt_utils.replace(field, "'http://'", "''"
                ), "'https://'", "''"
            ), "'/'", 1
        ), "'?'", 1
    )
    
-%}

     
    {{ dbt_utils.safe_cast(
        parsed,
        dbt_utils.type_string()
        )}}
        

{%- endmacro %}