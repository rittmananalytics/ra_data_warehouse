with source as (
  select * from {{ source('activecampaign', 'campaign_messages') }}
),

renamed as (
  select
      SAFE_CAST(messageid as INT64) as message_id,
      SAFE_CAST(campaignid as INT64) as campaign_id,
      SAFE_CAST(percentage as INT64) as percentage,
      SAFE_CAST(sourcesize as INT64) as source_size,
      SAFE_CAST(initial_split_percentage as INT64) as initial_split_percentage,
      SAFE_CAST(subject as STRING) as message_subject,
    _sdc_batched_at,
   max(_sdc_batched_at) over (partition by messageid order by _sdc_batched_at range between unbounded preceding and unbounded following) as max_sdc_batched_at
  from source
)

select
  *
  EXCEPT (_sdc_batched_at, max_sdc_batched_at)
from renamed
where _sdc_batched_at = max_sdc_batched_at
