{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("product_warehouse_event_sources") %}
{% if 'rudderstack_events_page' in var("product_warehouse_event_sources") %}
{{
    config(
        materialized="table"
    )
}}

with order_lines AS (

  SELECT * FROM {{ source('custom_events', 'order_lines') }}

),
    orders AS (

  SELECT * FROM {{ source('custom_events', 'orders') }}

),
    user_registrations AS (

  SELECT * FROM {{ source('custom_events', 'user_registrations') }}

),
with agg_order_lines as (
    select
        order_id,
        sum(net_price_global_currency) as net_price_global_currency,
        sum(net_price_local_currency) as net_price_local_currency
    from order_lines
    group by order_id),
    customers as (
    select
    cast(cast(d_user_id as integer) as varchar) as user_id,
    joined_time
from user_registrations),
    orders as (
      select
        *
      from
        orders
    ),
joined as (
  select
    cast(fo.order_id as string) as event_id,
    'confirmed_order'           as event_type,
    fo.last_checkout_time       as event_ts,
    cast(agg.net_price_global_currency as string) as event_details,
    cast(null as string)        as page_title,
    cast(null as string)  as page_url_path,
    cast(null as string)  as referrer_host,
    cast(null as string)  as search,
    cast(null as string)  as PAGE_URL ,
	  cast(null as string) as PAGE_URL_HOST ,
	  cast(null as string)  as GCLID ,
	  cast(null as string)  as UTM_TERM ,
	  cast(null as string)  as UTM_CONTENT ,
  	cast(null as string)  as UTM_MEDIUM ,
  	cast(null as string)  as UTM_CAMPAIGN ,
  	cast(null as string)  as UTM_SOURCE ,
  	cast(null as string)  as IP ,
  	cast(cast(fo.d_customer_user_id as integer) as varchar) as VISITOR_ID,
  	cast(cast(fo.d_customer_user_id as integer) as varchar) as user_id,
  	cast(null as string)  as DEVICE ,
  	'Orders DB' as site,
	  cast(null as string)as DEVICE_CATEGORY
  from orders as fo
left join agg_order_lines as agg
on agg.order_id = fo.order_id
where is_confirmed_and_not_fixup_order)
,
signups as (
select
    md5(d_user_id) as event_id,
    'user_registration' as event_type,
    joined_time as event_ts,
    cast(null as string) as event_details,
    cast(null as string) as page_title,
    cast(null as string) as page_url_path,
    cast(null as string) as referrer_host,
    cast(null as string) as search,
    cast(null as string) as PAGE_URL ,
	cast(null as string) as PAGE_URL_HOST ,
	cast(null as string) as GCLID ,
	null::varchar as UTM_TERM ,
	null::varchar as UTM_CONTENT ,
	null::varchar as UTM_MEDIUM ,
	null::varchar as UTM_CAMPAIGN ,
	null::varchar as UTM_SOURCE ,
	null::varchar as IP ,
	d_user_id  as VISITOR_ID,
	d_user_id  as user_id,
	null::varchar as DEVICE ,
	'D_USERS' as site,
	null::varchar as DEVICE_CATEGORY
from customers c),
unioned as (
    select
        *
    from
        joined
    union all
    select
        *
    from
        signups
)
select
    *
   from
   unioned
