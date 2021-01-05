{% if var("product_warehouse_event_sources") %}

{{
    config(
        alias='web_sessions_fact'
    )
}}

with sessions as
  (
    SELECT
      *
    FROM (
      SELECT
        session_id,
        session_start_ts,
        session_end_ts,
        events,
        utm_source,
        utm_content,
        utm_medium,
        utm_campaign,
        utm_term,
        search,
        gclid,
        first_page_url,
        first_page_url_host,
        first_page_url_path,
        referrer_host,
        device,
        device_category,
        last_page_url,
        last_page_url_host,
        last_page_url_path,
        duration_in_s,
        duration_in_s_tier,
        referrer_medium,
        referrer_source,
        channel,
        blended_user_id,
        sum(mins_between_sessions) over (partition by session_id) as mins_between_sessions,
        is_bounced_session
      FROM
        {{ ref('int_web_events_sessions_stitched') }}
      )
      {{ dbt_utils.group_by(n=28) }}
    )
    {% if var('marketing_warehouse_ad_campaign_sources') %}
        ,
      utm_campaign_mapping as
      ( SELECT
          *
        FROM
          {{ ref('utm_campaign_mapping')}}
      ),
      ad_campaigns as (
        SELECT
          *
        FROM
          {{ ref('wh_ad_campaigns_dim')}}
      )
    {% endif %}
    {% if var("subscriptions_warehouse_sources")  %}
      ,
      customers as (
       SELECT
        *
       FROM   {{ ref('wh_customers_dim') }}
         )
    {% endif %}
      ,
      joined as (
        SELECT
            s.*
            {% if var('marketing_warehouse_ad_campaign_sources') %}
            ,a.ad_campaign_pk
            {% endif %}
            {% if var("subscriptions_warehouse_sources")  %}
            ,c.customer_pk
            {% endif %}
        FROM
           sessions s
    {% if var("subscriptions_warehouse_sources")  %}
        LEFT OUTER JOIN
          customers c
        ON
          s.blended_user_id = c.customer_id
        {% endif %}
        {% if var('marketing_warehouse_ad_campaign_sources') %}
         LEFT OUTER JOIN
          utm_campaign_mapping m
         ON
          s.utm_campaign = m.utm_campaign
        LEFT OUTER JOIN
          ad_campaigns a
        ON m.ad_campaign_id = a.ad_campaign_id
        {% endif %}
      ),
      ordered as (
        {% if target.type == 'bigquery' %}
        SELECT
          {{ dbt_utils.surrogate_key(['to_hex(session_id)']) }} as web_sessions_pk,
      {% else %}
        SELECT
          {{ dbt_utils.surrogate_key(['session_id']) }} as web_sessions_pk,
      {% endif %}
          * ,
          row_number() over (partition by blended_user_id order by session_start_ts) as user_session_number
        FROM
          joined)
  SELECT
    *
  FROM
    ordered

{% else %}

{{config(enabled=false)}}

{% endif %}
