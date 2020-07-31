{%- macro generate_tests_three() -%}




{% import 'manifest.json' as manifest %}




{% if execute %}

{{ log( manifest , info=True) }}
{% do return(manifest) %}

{% endif %}



{%- endmacro -%}
