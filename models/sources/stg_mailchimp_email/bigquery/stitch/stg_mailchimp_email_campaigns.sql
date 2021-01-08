{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_email_event_sources") %}
{% if 'mailchimp_email' in var("marketing_warehouse_email_event_sources") %}

with source as (
  SELECT
    id,
    concat('mailchimp-',id) AS ad_campaign_id,
    settings.subject_line AS ad_campaign_name,
    status AS ad_campaign_status,
    CAST (NULL AS string) AS campaign_buying_type,
    content_type AS campaign_content_type,
    {{ dbt_utils.date_trunc('DAY', 'send_time') }} AS ad_campaign_start_date,
    {{ dbt_utils.date_trunc('DAY', 'send_time') }} AS ad_campaign_end_date,
    'Mailchimp' AS ad_network
  FROM `ra-development.stitch_mailchimp.campaigns`
  group by 1,2,3,4,5,6,7,8
),
renamed as (
  SELECT
    ad_campaign_id,
    ad_campaign_name,
    ad_campaign_status,
    campaign_buying_type,
    ad_campaign_start_date,
    ad_campaign_end_date,
    ad_network
  FROM
    source
)
select *
from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
