{% if var('marketing_warehouse_ad_sources') %}

with ads as
  (
    {% for source in var('marketing_warehouse_ad_sources') %}
      {% set relation_source = 'stg_' + source %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
 {% if var('marketing_warehouse_ad_group_sources') %}
 ,ad_groups as (
   SELECT {{ dbt_utils.star(from=ref('int_ad_ad_groups')) }}
   FROM
   {{ ref('int_ad_ad_groups') }}
 ),
 {% endif %}
 {% if var('marketing_warehouse_ad_campaign_sources') %}
 ad_campaigns as (
   SELECT {{ dbt_utils.star(from=ref('int_ad_campaigns')) }}
   FROM
   {{ ref('int_ad_campaigns') }}
 )
 {% endif %}
select
    a.ad_id,
    a.ad_status,
    a.ad_type,
    a.ad_final_urls,
    a.ad_group_id,
    a.ad_bid_type,
    a.ad_utm_parameters,
  {% if var('marketing_warehouse_ad_campaign_sources') and var('marketing_warehouse_ad_group_sources') %}
    lower(coalesce(a.ad_utm_campaign,c.ad_campaign_name)) as ad_utm_campaign,
  {% else %}
    a.ad_utm_campaign as ad_utm_campaign,
  {% endif %}
    lower(a.ad_utm_content) as ad_utm_content,
    coalesce(a.ad_utm_medium,'paid') as ad_utm_medium,
    case when a.ad_network = 'Google Ads' then coalesce(a.ad_utm_source,'adwords')
         when a.ad_network = 'Facebook Ads' then coalesce(a.ad_utm_source,'facebook')
         end as ad_utm_source,
    a.ad_network
from ads a
{% if var('marketing_warehouse_ad_campaign_sources') and var('marketing_warehouse_ad_group_sources') %}
left outer join ad_groups g
on a.ad_group_id = g.ad_group_id
left outer join ad_campaigns c
on g.ad_campaign_id = c.ad_campaign_id
{% endif %}
{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
