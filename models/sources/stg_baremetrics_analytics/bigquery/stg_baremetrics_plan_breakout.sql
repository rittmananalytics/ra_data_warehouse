{{config(enabled = target.type == 'bigquery')}}
{% if var("subscriptions_warehouse_sources") %}
{% if 'baremetrics_analytics' in var("subscriptions_warehouse_sources") %}

with source as (
  select * from ra-development.baremetrics.plan_breakout
),
renamed as (
SELECT
       timestamp(human_date) as plan_breakout_ts,
       concat('stripe-',oid) as plan_id,
       max(case when metric = 'arpu' then value end) as plan_arpu,
       max(case when metric = 'ltv' then value end) as plan_ltv,
       max(case when metric = 'cancellations' then value end) as plan_cancellations,
       max(case when metric = 'reactivations' then value end) as plan_reactivations,
       max(case when metric = 'mrr' then value end) as plan_mrr
FROM source
group by 1,2)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
