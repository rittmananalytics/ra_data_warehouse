select * except (_fivetran_synced,max_fivetran_synced)
from (
SELECT
pk as usage_log_id,
client AS customer_name,
created_time as usage_created_time,
dialect as usage_t_database_type,
explore as usage_subject_area,
issuer_source usage_originator_type,
name AS user_name,
rebuild_pdts_yes_no_ as usage_triggered_cache_reload,
source as usage_category,
status as usage_status,
title as usage_feature_title,
approximate_web_usage_in_minutes as usage_time_elapsed_mins,
average_runtime_in_seconds as usage_response_time_secs,
_fivetran_synced,
max(_fivetran_synced) OVER (PARTITION BY pk ORDER BY _fivetran_synced RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_fivetran_synced
FROM
`ra-development`.`fivetran_email`.`usage_stats`)
where _fivetran_synced = max_fivetran_synced
