{% if var("subscriptions_warehouse_sources")  %}

{{
    config(
      alias='attribution_fact'
    )
}}

WITH subscription_details AS
    (
      SELECT {{ dbt_utils.star(from=ref('int_converter_subscription_revenue')) }}
      FROM
        {{ref ('int_converter_subscription_revenue') }} ),
converting_events as
    (
      SELECT
        e.blended_user_id,
        first_value(CASE WHEN event_type = '{{ var('attribution_conversion_event_type') }}' THEN session_id END) over (PARTITION BY e.blended_user_id order by e.event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as session_id,
        s.* EXCEPT (event_id,user_id),
        event_type,
        MIN(CASE WHEN event_type = '{{ var('attribution_conversion_event_type') }}' THEN event_ts END ) OVER (PARTITION BY e.blended_user_id) AS converted_ts,
        MIN(CASE WHEN event_type = '{{ var('attribution_create_account_event_type') }}' THEN event_ts END ) OVER (PARTITION BY e.blended_user_id) AS created_account_ts
      FROM
        {{ref ('wh_web_events_fact') }} e
      LEFT OUTER JOIN
        subscription_details s
      ON
        e.event_id = s.event_id
      WHERE
        event_type = '{{ var('attribution_conversion_event_type') }}'
        OR event_type = '{{ var('attribution_create_account_event_type') }}'),
converting_sessions as (
    SELECT
      *
    FROM
      converting_events
    {{ dbt_utils.group_by(12) }}
  ),
converting_sessions_deduped as (
    SELECT
      blended_user_id AS blended_user_id,
      MAX(CASE WHEN event_type = '{{ var('attribution_conversion_event_type') }}' THEN session_id END ) AS session_id,
      MAX(plan_id) AS plan_id,
      MIN(plan_name) AS plan_name,
      MIN(plan_interval_count) AS plan_interval_count,
      MIN(plan_amount) AS plan_amount,
      MAX(baremetrics_predicted_ltv) AS baremetrics_predicted_ltv,
      MIN(converted_ts) AS converted_ts,
      MIN(created_account_ts) AS created_account_ts
    FROM
      converting_sessions
    GROUP BY
     1
  ),
converting_sessions_deduped_labelled as
    (
      SELECT
        c.blended_user_id,
        MAX(c.plan_amount) OVER (PARTITION BY c.blended_user_id) AS plan_amount,
        MAX(c.baremetrics_predicted_ltv) OVER (PARTITION BY c.blended_user_id) AS baremetrics_predicted_ltv,
        s.session_start_ts,
        s.session_end_ts,
        c.converted_ts,
        c.created_account_ts,
        s.session_id AS session_id,
        ROW_NUMBER() OVER (PARTITION BY c.blended_user_id ORDER BY s.session_start_ts) AS session_seq,
        CASE WHEN c.created_account_ts BETWEEN s.session_start_ts AND s.session_end_ts THEN TRUE ELSE FALSE END AS account_opening_session,
        CASE WHEN (c.converted_ts BETWEEN s.session_start_ts AND s.session_end_ts)  THEN TRUE ELSE FALSE END AS conversion_session,
        CASE WHEN (c.converted_ts BETWEEN s.session_start_ts AND s.session_end_ts)  THEN 1 ELSE 0 END AS event,
        CASE WHEN s.session_start_ts BETWEEN c.created_account_ts AND coalesce(c.converted_ts, s.session_end_ts) THEN TRUE ELSE FALSE END AS trialing_session,
        utm_source,
        utm_content,
        utm_medium,
        utm_campaign,
        referrer_host,
        first_page_url_host,
        split(net.reg_domain(referrer_host),'.')[OFFSET(0)] as referrer_domain,
        channel,
        events
      FROM
        {{ ref('wh_web_sessions_fact') }} s
      JOIN
        converting_sessions_deduped c
      ON
        c.blended_user_id = s.blended_user_id
      WHERE
        c.converted_ts >= s.session_start_ts
      ORDER BY
        c.blended_user_id,
        s.session_start_ts),
session_attrib_pct as (
    SELECT
      * except (first_page_url_host),
      CASE
        WHEN session_id = LAST_VALUE(session_id) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
      ELSE
      0
    END
      AS LAST_click_attrib_pct,
      CASE
        WHEN session_id = FIRST_VALUE(session_id) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
      ELSE
      0
    END
      AS first_click_attrib_pct,
      1/COUNT(session_id) OVER (PARTITION BY blended_user_id) AS even_click_attrib_pct,
      CASE
        WHEN session_start_ts = FIRST_VALUE(session_start_ts) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts) AND MAX(event) OVER (PARTITION BY blended_user_id) = 1 THEN SAFE_CAST(1.1-ROW_NUMBER() OVER (PARTITION BY blended_user_id) AS STRING)
        WHEN session_start_ts > LAG(session_start_ts) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts)
      AND MAX(event) OVER (PARTITION BY blended_user_id) = 1 THEN SAFE_CAST(ROUND(1.1-1/ROW_NUMBER() OVER (PARTITION BY blended_user_id), 2) AS STRING)
      ELSE
      'null'
    END
      AS weights
    FROM  converting_sessions_deduped_labelled),
session_attrib_pct_with_time_decay AS (
    SELECT
      * EXCEPT (weights),
      ROUND(IF (SAFE_CAST(weights AS FLOAT64)=0 OR SUM(SAFE_CAST(weights AS FLOAT64)) OVER (PARTITION BY blended_user_id)=0, 0, SAFE_CAST(weights AS FLOAT64)/SUM(SAFE_CAST(weights AS FLOAT64)) OVER (PARTITION BY blended_user_id)), 2) AS time_decay_attrib_pct
    FROM
      session_attrib_pct),
final as (
    SELECT
      *,
      round(MAX(plan_amount * first_click_attrib_pct),2) AS first_click_attrib_first_plan,
      round(MAX(plan_amount * last_click_attrib_pct),2) AS last_click_attrib_first_plan,
      round(MAX(plan_amount * even_click_attrib_pct),2) AS even_click_attrib_first_plan,
      round(MAX(plan_amount * time_decay_attrib_pct),2) AS time_decay_attrib_first_plan,
      round(MAX(baremetrics_predicted_ltv * first_click_attrib_pct),2) AS first_click_attrib_baremetrics_predicted_ltv,
      round(MAX(baremetrics_predicted_ltv * last_click_attrib_pct),2) AS last_click_attrib_baremetrics_predicted_ltv,
      round(MAX(baremetrics_predicted_ltv * even_click_attrib_pct),2) AS even_click_attrib_baremetrics_predicted_ltv,
      round(MAX(baremetrics_predicted_ltv * time_decay_attrib_pct),2) AS time_decay_attrib_baremetrics_predicted_ltv
    FROM
      session_attrib_pct_with_time_decay
    {{ dbt_utils.group_by(25) }} )
select
  *
from
  final

  {% else %}

     {{config(enabled=false)}}

  {% endif %}
