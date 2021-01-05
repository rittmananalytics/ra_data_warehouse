{% if var("subscriptions_warehouse_sources")  %}

SELECT
    d.event_id AS event_id,
    d.user_id,
    max(d.event_details) as plan_id,
    max(d.event_ts) AS subscribe_event_ts,
    max(p.plan_interval) as plan_interval,
    max(p.plan_name) as plan_name,
    max(p.plan_interval_count) as plan_interval_count,
    max(p.plan_amount/100) AS plan_amount,
    max(b.plan_ltv/100) AS baremetrics_predicted_ltv
  FROM
    {{ ref('stg_segment_dashboard_events_events') }} d
  JOIN
    {{ ref('stg_stripe_subscriptions_plans') }} p
  ON
    d.event_details = p.plan_id
  JOIN
    {{ ref('stg_baremetrics_plan_breakout') }} b
  ON
    p.plan_id = b.plan_id
  WHERE
    d.event_type = 'subscribed'
    AND date(b.plan_breakout_ts) = date(d.event_ts)
    {{ dbt_utils.group_by(n=2) }}

    {% else %} {{config(enabled=false)}} {% endif %}
