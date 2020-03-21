{{
    config(
        materialized='table'
    )
}}

with base_hubspot_owners as (

    select * from {{ ref('hubspot_owners') }}

 ),

hubspot_owners as (

    select
    
      safe_cast (ownerid as int64) as ownerid,
      concat(concat(firstname,' '),lastname) as owner_fullname,
      firstname,
      lastname,
      concat(concat(firstname,' '),lastname) as salesperson_full_name,
      email,
      _sdc_batched_at,
      max(_sdc_batched_at) over (partition by ownerid order by _sdc_batched_at range between unbounded preceding and unbounded following) as latest_sdc_batched_at

    from base_hubspot_owners

)

select * from hubspot_owners
where _sdc_batched_at = latest_sdc_batched_at
