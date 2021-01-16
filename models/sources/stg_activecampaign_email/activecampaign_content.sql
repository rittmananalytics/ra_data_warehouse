with source as (
  select
    campaign_name,
    campaign_created_date,
    campaign_send_date,
    campaign_id,
    campaign_author_id
   from {{ ref('base_activecampaign_campaigns') }}
),

messages as (
  select * EXCEPT (highest_percentage,percentage)  from (
    select
      campaign_id,
      message_id,
      message_subject,
      max(percentage) over (partition by campaign_id) as highest_percentage,
      percentage
    from {{ ref('base_activecampaign_messages') }}
    )
  where percentage = highest_percentage
),

--This part structures the table so it can be unioned in content_dim
renamed as (
  select
    cast(null as string) as video_id,
    MAX(campaign_name) as content_name,
    concat("campaign-", s.campaign_id) AS content_id,
    MAX(cast(null as string)) as channel_name,
    MIN("email campaign") as content_type,
    MAX(cast(null as string)) as public_url,
    MIN(campaign_created_date) as date_published,
    MAX(cast(null as string)) as language,
    NULL as content_status,
    MAX(m.message_subject) as subject,
    cast(null as string) as appcontent_id,
    NULL as words_count,
 --   cast(null as string) AS fk_from_url,
    cast(null as string) AS blog_market
  from source s
  LEFT join messages m
  on s.campaign_id = m.campaign_id
  group by s.campaign_id
)

select
  *
from renamed
