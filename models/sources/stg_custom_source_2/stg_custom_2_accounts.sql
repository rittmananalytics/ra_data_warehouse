{% if not var("enable_custom_source_2") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    select *
    from
    {{ source('custom_source_2','s_accounts' ) }}
),
renamed as
(
select  concat('custom_2-',id) as       account_id,
        cast (null as string) as        account_name,
        cast (null as string) as        account_code,
        cast (null as string) as        account_type,
        cast (null as string) as        account_class,
        cast (null as string) as        account_status,
        cast (null as string) as        account_description,
        cast (null as string) as        account_reporting_code,
        cast (null as string) as        account_reporting_code_name,
        cast (null as string) as        account_currency_code,
        cast (null as string) as        account_bank_account_type,
        cast (null as string) as        account_bank_account_number,
        cast (null as string) as        account_is_system_account,
        cast (null as string) as        account_tax_type,
        cast (null as string) as        account_show_in_expense_claims,
        cast (null as string) as        account_enable_payments_to_account
from source
)
select * from renamed
