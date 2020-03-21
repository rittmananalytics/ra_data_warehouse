{{
    config(
        materialized='table'
    )
}}

with deal_pipelines as (

    select * from {{ ref('hubspot_deal_pipelines') }}

),

deal_stage_with_max as (

    select
      *,
      max(_sdc_batched_at) over (partition by pipelineid order by _sdc_batched_at range between unbounded preceding and unbounded following) as latest_sdc_batched_at

    from deal_pipelines

),

latest_version as (

    select * from deal_stage_with_max
    where _sdc_batched_at = latest_sdc_batched_at

),

final as (

    select

      label as pipeline_label,
      pipelineid,
      displayorder as pipeline_displayorder,
      active as pipeline_active

    from latest_version

)

select * from final
