{{config(enabled = target.type == 'snowflake')}}
with base as (

    select *
    from {{ ref('int__facebook_ads__carousel_media_prep') }}

), fields as (

    select
        _fivetran_id,
        creative_id,
        caption,
        description,
        message,
        link,
        index
    from base

)

select *
from fields
