{% if not var("enable_football_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='football_media_articles_fact',
        unique_key='football_media_article_pk'
    )
}}
{% endif %}

with articles as
(

  select * from {{ ref('int_football_media_articles') }}

)

select
  {{ dbt_utils.surrogate_key(
    ['url']
  ) }} as football_media_article_pk,
  url,

  media_date,
  publication_ts,

  celebrity_code_names,
  tag,
  title,
  description,

  title_score,
  description_score

from articles
