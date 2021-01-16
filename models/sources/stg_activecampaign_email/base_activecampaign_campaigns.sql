with source as (
  select * from {{ source('activecampaign', 'campaigns') }}
),

renamed as (
  select
    name as campaign_name,
    SAFE_CAST(cdate as	TIMESTAMP) as	 campaign_created_date,
    SAFE_CAST(sdate as	TIMESTAMP) as campaign_send_date,
    SAFE_CAST(id as INT64) as campaign_id,
    SAFE_CAST(userid as INT64) as campaign_author_id,
    SAFE_CAST(uniqueopens as INT64) as unique_opens,
    SAFE_CAST(opens as INT64) as opens,
    SAFE_CAST(uniquereplies as INT64)  as unique_replies,
    SAFE_CAST(subscriberclicks as INT64) as subscriber_clicks,
    SAFE_CAST(uniquelinkclicks as INT64) unique_link_clicks,
    SAFE_CAST(linkclicks as INT64) as link_clicks,
    SAFE_CAST(send_amt as INT64) as number_of_sends,
    SAFE_CAST(total_amt as INT64) as total_sends,
    SAFE_CAST(unsubreasons as INT64) as unsub_reasons,
    SAFE_CAST(unsubscribes as INT64) as unsubscribes,
    SAFE_CAST(softbounces as INT64) as soft_bounces,
    SAFE_CAST(hardbounces as INT64)  as hard_bounces,
    SAFE_CAST(uniqueforwards as INT64) as unique_forwards,
    SAFE_CAST(forwards as INT64) as forwards,
    SAFE_CAST(socialshares as INT64)  as social_shares,
    SAFE_CAST(replies as INT64) as replies,
    SAFE_CAST(willrecur as INT64) as will_recur,
    recurring,
    _sdc_batched_at,
   max(_sdc_batched_at) over (partition by id order by _sdc_batched_at range between unbounded preceding and unbounded following) as max_sdc_batched_at
  from source
)

select
  *
  EXCEPT (_sdc_batched_at, max_sdc_batched_at),
from renamed
where _sdc_batched_at = max_sdc_batched_at
