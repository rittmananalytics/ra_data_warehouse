{% if not var("enable_hubspot_crm_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (

      select *  from (
        select *,
        MAX(_sdc_batched_at)
         OVER (PARTITION BY dealid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
         AS max_sdc_batched_at
         from {{ source('hubspot_crm', 'deals') }})
          where max_sdc_batched_at = _sdc_batched_at


),

hubspot_deal_pipelines_source as (

    SELECT * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
    FROM
    (
      SELECT *,
             MAX(_sdc_batched_at) OVER (PARTITION BY pipelineid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM {{ source('hubspot_crm', 'deal_pipelines') }}
    )
    WHERE _sdc_batched_at = max_sdc_batched_at

  ),

hubspot_deal_stages as (

    select
      pipelineid,
      stageid,
      probability,
      closedwon,
      stages.label as stage_label,
      stages.displayorder as stage_displayorder,
      concat (cast( pipelineid as string), cast (stageid as string)) as pk


    from hubspot_deal_pipelines_source,
    unnest (stages) stages
    group by 1,2,3,4,5,6,7

),

owners as (
  SELECT *
  FROM {{ ref('t_hubspot_crm_owners') }}
),

deals_renamed as (

  select
    dealid AS deal_id,
    properties.closed_lost_reason.value AS deal_closed_lost_reason,
    properties.dealname.value AS deal_name,
    cast(associations.associatedcompanyids[offset(off)] as string) as hubspot_company_id, -- added 18/12/2019
    properties.hubspot_owner_id.value AS deal_owner_id,
    properties.hs_lastmodifieddate.value AS deal_last_modified_ts,
    properties.dealstage.value AS deal_stage_name,
    properties.dealstage.value as deal_stage_id,
    properties.dealstage.timestamp as deal_stage_ts,
    properties.pipeline.value AS deal_pipeline_name,
    properties.closedate.value AS deal_closed_date,
    properties.createdate.value AS deal_created_ts,
    properties.amount_in_home_currency.value AS deal_amount,
    properties.amount_in_home_currency.value AS deal_local_amount,
    properties.description.value AS deal_description,
    properties.dealstage.sourceid as deal_salesperson_email, -- added 06/11/2019
    properties.pricing_model.value AS deal_pricing_model, -- added 18/12/2019
    properties.source.value as deal_source,
    properties.products_in_solution.value as deal_products_in_solution,
    properties.sprint_type.value as deal_sprint_type,
    properties.days_to_close.value as deal_days_to_close,
    properties.partner_referral.value as deal_partner_referral_type,
    properties.deal_components.value as deal_components,
    properties.dealtype.value as deal_type,
    properties.assigned_consultant.value as deal_assigned_consultant,
    timestamp_millis(safe_cast(properties.delivery_start_date.value as int64)) as deal_delivery_start_ts,
    timestamp_millis(safe_cast(properties.delivery_schedule_date.value as int64)) as deal_delivery_schedule_ts,
    properties.number_of_sprints.value deal_number_of_sprints,
    properties.number_of_sprints.value * 14 as deal_duration_days,
    properties.deal_components.value as deal_count_components,

    _sdc_batched_at

  from source,
            unnest(associations.associatedcompanyids) with offset off

),

joined as (

    select
    concat('hubspot-',d.hubspot_company_id) as company_id,
    d.* except(hubspot_company_id),
    s.probability as deal_probability_pct,
    s.stage_label as deal_stage_label,
    s.stage_displayorder as deal_stage_display_order,
    p.label as pipeline_label,
    timestamp_diff(current_timestamp,d.deal_delivery_start_ts,DAY) as deal_days_until_end,
    timestamp(date_add(date(d.deal_delivery_start_ts), interval safe_cast(d.deal_duration_days as int64) day)) as deal_delivery_end_date_ts,
    case when s.stage_label like '%Closed Won%' then true else false end AS deal_is_closed, -- added 18/12/2019
    case when (s.stage_label in ('Closed Won and Scheduled','Verbally Won and Working at Risk')
    and timestamp_diff(current_timestamp,deal_delivery_start_ts,DAY) < 365/2)
    or
      (s.stage_label in ('Closed Won and Scheduled','Verbally Won and Working at Risk')
       and
       d.deal_delivery_start_ts < current_timestamp
       and date_add(date(d.deal_delivery_start_ts), interval safe_cast(d.deal_duration_days as int64) day) > current_date)
      then true else false end as is_active

     from renamed d
    left join hubspot_deal_stages s on d.deal_stage_id = s.stageid
    left join hubspot_deal_pipelines_source p on s.pipelineid = p.pipelineid
    left outer join owners u
    on safe_cast(d.deal_owner_id as int64) = u.ownerid

)

select * from joined
