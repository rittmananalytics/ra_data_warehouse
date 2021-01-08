{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_email_list_sources") %}
{% if 'mailchimp_email' in var("marketing_warehouse_email_list_sources") %}

WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_mailchimp_email_stitch_lists_table'),unique_column='id') }}
),
renamed as (
select
    concat('{{ var('stg_mailchimp_email_id-prefix') }}',id) as list_id,
    name as audience_name,
    stats.avg_sub_rate AS avg_sub_rate_pct,
    stats.avg_unsub_rate AS avg_unsub_rate_pct,
    stats.campaign_count AS total_campaigns,
    stats.campaign_last_sent AS campaign_last_sent_ts,
    stats.cleaned_count AS total_cleaned,
    stats.cleaned_count_since_send AS total_cleaned_since_send,
    stats.click_rate AS click_rate_pct,
    stats.last_sub_date AS last_sub_ts,
    stats.last_unsub_date AS last_unsub_ts,
    stats.member_count AS total_members,
    stats.member_count_since_send AS total_members_since_send,
    stats.merge_field_count AS total_merge_fields,
    stats.open_rate AS open_rate_pct,
    stats.target_sub_rate AS target_sub_rate_pct,
    stats.unsubscribe_count AS total_unsubscribes,
    stats.unsubscribe_count_since_send AS total_unsubscribes_since_send
from source)
select *
from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
