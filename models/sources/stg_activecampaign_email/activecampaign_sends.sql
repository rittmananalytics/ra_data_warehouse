--this is mainly a placeholder to minimise rework for when/if we decide to construct a send spline from campaigns & contactLists

with source as (
  select * from {{ ref('base_activecampaign_campaigns')}}
),

renamed as (
  select
    campaign_send_date,
    campaign_created_date,
    campaign_id,
    concat("campaign-", campaign_id) as content_id,
    unique_opens as unique_opens,
    opens,
    unique_replies,
    subscriber_clicks,
    unique_link_clicks,
    link_clicks,
    number_of_sends,
    total_sends,
    unsub_reasons,
    unsubscribes,
    soft_bounces,
    hard_bounces,
    unique_forwards,
    forwards,
    social_shares,
    replies
  from source
)

select
  *
from renamed
