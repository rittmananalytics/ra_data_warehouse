{% macro replace(field, old_chars, new_chars) %}

    replace(
        {{ field }},
        {{ old_chars }},
        {{ new_chars }}
    )


{% endmacro %}
