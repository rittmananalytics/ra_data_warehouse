{% if var('marketing_warehouse_ad_campaign_sources') %}


with campaigns as
  (
    {% for source in var('marketing_warehouse_ad_campaign_sources') %}
      {% set relation_source = 'stg_' + source + '_campaigns' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select *,

       case when ad_network = 'Google Ads' then 'adwords'
            when ad_network = 'Facebook Ads' then 'facebook'
            when ad_network = 'Mailchimp' then 'newsletter'
            when ad_network = 'Hubspot Email' then 'hs_email'
            end as utm_source,
       case when ad_network = 'Google Ads' then 'ppc'
            when ad_network = 'Facebook Ads' then 'paid_social'
            when ad_network in ('Mailchimp','Hubspot Email') then 'email'
            else null end as utm_medium,
       case when ad_campaign_name like '%Winter 2019%' then 'winter_2019'
            when ad_campaign_name like '%Summer 2020%' then 'summer_2020'
            when ad_campaign_name = 'Rittman Analytics Newsletter December 2020' then 'Analytics Solutions December 2020'
       else lower(ad_campaign_name) end as utm_campaign
 from campaigns

 {% else %}

 {{
     config(
         enabled=false
     )
 }}


 {% endif %}
