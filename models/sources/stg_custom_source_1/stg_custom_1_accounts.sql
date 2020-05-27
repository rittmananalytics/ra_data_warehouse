{% if not var("enable_custom_source_1") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    SELECT country_name, SUM(c) c
    FROM (
    SELECT ip, country_name, c
  FROM (
    SELECT *, NET.SAFE_IP_FROM_STRING(ip) & NET.IP_NET_MASK(4, mask) network_bin
    FROM source_of_ip_addresses, UNNEST(GENERATE_ARRAY(9,32)) mask
    WHERE BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(ip)) = 4
  )
  JOIN ``
  USING (network_bin, mask)
)
GROUP BY 1
ORDER BY 2 DESC
    {{ source('custom_source_1','s_accounts' ) }}
),
renamed as
(
select  concat('custom_1-',id)                             account_id,
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
