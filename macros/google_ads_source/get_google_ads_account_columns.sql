{% macro get_google_ads_account_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "account_label_id", "datatype": dbt_utils.type_int()},
    {"name": "account_label_name", "datatype": dbt_utils.type_string()},
    {"name": "can_manage_clients", "datatype": "boolean"},
    {"name": "currency_code", "datatype": dbt_utils.type_string()},
    {"name": "date_timezone", "datatype": dbt_utils.type_string()},
    {"name": "id", "datatype": dbt_utils.type_int()},
    {"name": "manager_customer_id", "datatype": dbt_utils.type_int()},
    {"name": "name", "datatype": dbt_utils.type_string()},
    {"name": "sequence_id", "datatype": dbt_utils.type_int()},
    {"name": "test_account", "datatype": "boolean"}
] %}

{{ return(columns) }}

{% endmacro %}
