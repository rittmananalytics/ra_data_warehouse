{% if (not var("enable_segment_events_source") and var("enable_mixpanel_events_source")) or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='web_events_rpt'
    )
}}
{% endif %}

SELECT s.*,
ARRAY_AGG(STRUCT(e.event_in_session_seq as session_event_num, e.event_seq as event_num, e.city as city,	e.countryLabel as country,	case when e.latitude is not null then concat(e.latitude,',',e.longitude) end as lat_long, e.event_type as event_type, case when e.event_type = 'Page View' then 1 end as page_view_count, e.event_ts as event_ts, e.event_details as event_details, e.page_url as page_url, e.time_on_page_secs as time_on_page_secs,
                 case when p.page_url_host in ('rittmananalytics.com','drilltodetail.rittmananalytics.com') then p.page_url_host else 'dev.rittmananalytics.com' end as page_url_host, page_title as page_title, page_url_path as page_url_path)) as event
FROM {{ ref('wh_web_sessions_fact') }} s
join {{ ref('wh_web_events_fact') }} e
on s.session_id = e.session_id
left outer join {{ ref('wh_web_pages_dim') }} p
on e.web_page_pk = p.web_page_pk
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
order by 4
