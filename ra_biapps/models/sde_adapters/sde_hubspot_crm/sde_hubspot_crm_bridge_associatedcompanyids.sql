with deals as (

        select * from {{ ref('hubspot_deals') }}

),

deal_stage_with_max as (

       select
        *,
        max(_sdc_batched_at) over (partition by dealid order by _sdc_batched_at range between unbounded preceding and unbounded following) as latest_sdc_batched_at

       from deals

),

latest_version as (

        select * from deal_stage_with_max
        where _sdc_batched_at = latest_sdc_batched_at

),

new_deal as (

    select
      associations.associatedcompanyids[offset(off)] as associatedcompanyids,
      dealid as deal_id

    from latest_version,
              unnest(associations.associatedcompanyids) with offset off

),

pk as (

    select
      *,
      FORMAT('%i_%i', associatedcompanyids , deal_id ) as pk

     from new_deal
)

select * from pk
