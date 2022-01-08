{% if var("marketing_warehouse_ad_sources") %}

/* Start by taking the unioned ad network stats, aggregated to day, platform, account, campaign and ad group level
for clicks, impressions and spend, calculating cost per-click and click-through rate and labelling these
as "reported" stats, in-contrast to the "actual" stats we'll observe using Snowplow.

Note: there's an assumption that there's only one account per platform, with platform = Ad Network (Google Ads, FB Ads etc)
Note: these ad network stats only cover Facebook Ads and Google Ads for now */

with stats as (
  select
    date_day as campaign_ts,
    platform,
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    sum(clicks) as reported_clicks,
    sum(impressions) as reported_impressions,
    sum(spend) as reported_spend,
    DIV0(sum(spend),sum(clicks)) as reported_cpc,
    DIV0(sum(clicks),sum(impressions)) as reported_ctr
from
  {{ ref('int_ad_reporting') }}
group by
  1,2,3,4,5,6,7,8
),

/* now get the list of GCLID > ad_group & campaign lookup values, so that we can link incoming
clicks from Snowplow with the marketing activity that they were the result of */

ad_click_ids as (
  select
    gclid,
    ad_group_id,
    campaign_id
  from
    {{ ref('int_ad_click_ids') }}
  group by
    1,2,3
),

/* roll-up sessions to the same level of aggregation (day) as the ad network stats + spend data, and
use the ad_click_ids lookup CTE to tag those sessions with ad campaign and ad group IDs where possible */

sessions as (
  select
    date_trunc('DAY',session_start_ts) as campaign_ts,
    case
      when first_page_url ilike '%fbclid%' or utm_source ilike '%facebook%' then 'Facebook Ads'
      when s.gclid is not null or utm_source ilike '%google%' then 'Google Ads'
      else null end
    as platform,
    coalesce(i.campaign_id,split_part(split_part(split_part(first_page_url,'clid=',2),'&',1),'_',1)) as campaign_id,
    coalesce(i.ad_group_id,split_part(split_part(split_part(first_page_url,'clid=',2),'&',1),'_',2)) as ad_group_id,
    count(distinct web_session_pk) as actual_clicks
  from
    {{ ref('int_web_sessions') }} s
  left join
    ad_click_ids i
  on s.gclid = i.gclid
    group by 1,2,3,4
  ),

/* Now we have the actual number of clicks (from Snowplow), we can calculate the actual CTR and CPC */

stats_plus_actual_clicks as (
  select
    c.*,
    coalesce(actual_clicks,0) as actual_clicks,
    DIV0(reported_spend,coalesce(actual_clicks,0)) as actual_cpc,
    DIV0(actual_clicks,reported_impressions) as actual_ctr
  from
    stats c

/*  We match to the ad network data using the "clid", a concatenated combination of campaignid_adsetid_adid that we obtain

    by matching by click ID (GCLID for now) and using the campaign_id and adset_id that apply to that click ID,  or if a click ID can't be
    found we then search for "clid" in the first_page_url and parse its contents into its campaign_id, adset_id (aka ad_group_id) and ad_id elements,

    Note: as Google Ads cost and other reported stats data is only available down to Ad Group level, we only report at that level and leave "adid" out of the join
    Note : as we can't reliably use utm_source to determine the Ad network (platform) value, we infer it using the presence of "gclid" or "fbclid" in the URL, or "google"/"facebook" in utm_source */

  left join
    sessions s
  on
    c.campaign_id = s.campaign_id
  and
    c.ad_group_id = s.ad_group_id
  and
    c.platform = s.platform
  and
    c.campaign_ts = s.campaign_ts
  ),

/* Finally we add two additional columns to the table, calculating for each day of each campaign/adset how many days it was since the first day we saw
data for that campaign, useful for analyzing the effectiveness of campaign and adset spend over time */

stats_plus_actual_clicks_plus_days_since as (
  select
    *,
    first_value(campaign_ts) over (partition by platform, campaign_id order by campaign_ts) as campaign_start_ts,
    first_value(campaign_ts) over (partition by platform, campaign_id, ad_group_id order by campaign_ts) as ad_group_start_ts,
    datediff('DAY',first_value(campaign_ts) over (partition by platform, campaign_id order by campaign_ts),campaign_ts) as days_since_campaign_start_ts,
    datediff('DAY',first_value(campaign_ts) over (partition by platform, campaign_id, ad_group_id order by campaign_ts),campaign_ts) as days_since_ad_group_start_ts
from
  stats_plus_actual_clicks)
select
  *
from
  stats_plus_actual_clicks_plus_days_since

{% else %} {{config(enabled=false)}} {% endif %}
