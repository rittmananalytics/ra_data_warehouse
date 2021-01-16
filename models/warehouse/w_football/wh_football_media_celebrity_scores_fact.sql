{% if not var("enable_football_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='football_media_celebrity_scores_fact',
        unique_key='football_media_celebrity_score_pk'
    )
}}
{% endif %}

with celebrity_scores as
(

  select * from {{ ref('int_football_media_celebrity_scores') }}

)

select
  {{ dbt_utils.surrogate_key(
    ['media_feed_celebrity_scores_natural_key']
  ) }} as football_media_celebrity_score_pk,
  media_feed_celebrity_scores_natural_key, 

  celebrity_code_name,
  celebrity_name, 
  media_date,

  game,
  article_urls,

  media_score

from celebrity_scores
