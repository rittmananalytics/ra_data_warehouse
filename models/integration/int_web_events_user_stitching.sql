{% if var('product_warehouse_event_sources') %}

with events AS (

    SELECT * FROM {{ ref('int_web_events') }}

),

mapping AS (

    SELECT distinct

        visitor_id AS visitor_id,

        last_value(user_id ignore nulls) over (
            PARTITION BYvisitor_id
            order by event_ts
            rows between unbounded preceding and unbounded following
        ) AS user_id,

        min(event_ts) over (
            PARTITION BYvisitor_id
        ) AS first_seen_at,

        max(event_ts) over (
            PARTITION BYvisitor_id
        ) AS last_seen_at

    FROM events

)

SELECT * FROM mapping

{% else %}

{{config(enabled=false)}}

{% endif %}
