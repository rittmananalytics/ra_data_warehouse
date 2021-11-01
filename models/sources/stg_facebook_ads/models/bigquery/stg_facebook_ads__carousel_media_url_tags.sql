{{config(enabled = target.type == 'bigquery')}}
with base as (

    select *
    from {{ ref('int__facebook_ads__carousel_media_prep') }}

), unnested as (

    select
        _fivetran_id,
        creative_id,
        index,
        json_extract_scalar(element, '$.key') as key,
        json_extract_scalar(element, '$.value') as value
    from base
    inner join unnest(url_tags) as element

)

select *
from unnested
