{% if var('product_warehouse_event_sources') %}

{% set sessionization_cutoff %}
(
    SELECT
        {{ dbt_utils.dateadd(
            'hour',
            -var('web_sessionization_trailing_window'),
            'max(session_start_ts)'
        ) }}
    FROM {{this}}
)
{% endset %}

{#
Window functions are challenging to make incremental. This approach grabs
existing values FROM the existing table and then adds the value of session_number
on top of that seed. During development, this decreased the model runtime
by 25x on 2 years of data (FROM 600 to 25 seconds), so even though the code is
more complicated, the performance tradeoff is worth it.
#}

with sessions AS (

    SELECT * FROM {{ref('int_web_events_sessions_stitched')}}

    {% if is_incremental() %}
    where CAST(session_start_ts AS datetime) > {{sessionization_cutoff}}
    {% endif %}

),

{% if is_incremental() %}

agg AS (

    SELECT
        blended_user_id,
        count(*) AS starting_session_number
    FROM {{this}}

    -- only include sessions that are not going to be resessionized in this run
    where CAST(session_start_ts AS datetime) <= {{sessionization_cutoff}}

    group by 1

),

{% endif %}

windowed AS (

    SELECT

        *,

        row_number() over (
            PARTITION BYblended_user_id
            order by sessions.session_start_ts
            )
            {% if is_incremental() %}+ coalesce(agg.starting_session_number, 0) {% endif %}
            AS session_number

    FROM sessions

    {% if is_incremental() %}
    left join agg using (blended_user_id)
    {% endif %}


)

SELECT * FROM windowed

{% else %}

{{config(enabled=false)}}

{% endif %}
