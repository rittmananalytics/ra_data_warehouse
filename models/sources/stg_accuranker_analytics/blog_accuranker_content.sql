{{
    config(
        materialized='table'
    )
}}

WITH accuranker_keywords AS (
SELECT 
id,
path AS blog_path, 
highest_ranking_page,
date_period AS date, 
keyword,
keyword_id,
avg_cost_per_click,
competition,
search_volume, 
starred,
lastest_date_period,
rank
FROM {{ ref('base_accuranker_keywords')}}),

accuranker_tags AS (
SELECT 
id,
ARRAY_AGG(tag) AS tag_list
FROM {{ ref('base_accuranker_keyword_tags')}}
GROUP by 1),

accuranker_lastest AS (
SELECT 
keyword_id,
rank AS latest_rank,
avg_cost_per_click AS latest_avg_cost_per_click,
competition AS latest_competition,
search_volume AS latest_search_volume, 
starred AS latest_starred
FROM accuranker_keywords
WHERE lastest_date_period = date
),

accuranker_window_functions AS ( 
SELECT 
accuranker_keywords.id,
accuranker_keywords.blog_path, 
highest_ranking_page,
date, 
keyword,
avg_cost_per_click,
competition,
search_volume, 
starred, 
rank,
ARRAY_TO_STRING(tag_list, ", ") AS tag_list,
latest_rank,
lastest_date_period,
latest_avg_cost_per_click,
latest_competition,
latest_search_volume, 
latest_starred,
max(accuranker_keywords.rank) OVER(PARTITION BY date,blog_path) AS max_rank_for_keyword_on_blog_path_by_date,
min(accuranker_keywords.rank) OVER(PARTITION BY date,blog_path) AS min_rank_for_keyword_on_blog_path_by_date,
rank () OVER(PARTITION BY date,blog_path order by accuranker_keywords.rank) AS keyword_rank_on_blog_path_by_date,
lag (accuranker_keywords.rank) OVER (PARTITION BY  keyword, blog_path ORDER BY date) AS preivous_day_rank,
lag (search_volume) OVER (PARTITION BY keyword, blog_path ORDER BY date) AS preivous_day_search_volume,
lag (avg_cost_per_click) OVER (PARTITION BY keyword, blog_path ORDER BY date) AS preivous_day_avg_cost_per_click,
lag (competition) OVER (PARTITION BY keyword, blog_path ORDER BY date) AS preivous_day_competition
FROM accuranker_keywords
LEFT JOIN accuranker_tags ON
accuranker_keywords.id = accuranker_tags.id
LEFT JOIN accuranker_lastest ON
accuranker_keywords.keyword_id = accuranker_lastest.keyword_id)

SELECT 
accuranker_window_functions.*,
date_published,
preivous_day_rank - rank AS rank_change_from_previous_day,
search_volume - preivous_day_search_volume AS search_volume_change_from_previous_day,
avg_cost_per_click - preivous_day_avg_cost_per_click AS avg_cost_per_click_change_from_previous_day,
competition - preivous_day_competition AS competition_change_from_previous_day
FROM 
accuranker_window_functions
LEFT JOIN {{ ref('blog_content_dim')}} AS blog_content_dim
ON accuranker_window_functions.blog_path = blog_content_dim.public_url
