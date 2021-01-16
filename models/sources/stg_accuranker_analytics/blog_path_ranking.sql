WITH landing_pages AS (
SELECT 
base_accuranker_keywords_rank.id_fk, 
base_accuranker_keywords_rank.keyword_id,
base_accuranker_keywords_rank.date_period,
base_accuranker_keywords_rank.keyword,
base_accuranker_keywords_rank.prefered_page,
base_accuranker_keywords_rank.landing_page,
base_accuranker_keywords_rank.highest_ranking_page,
base_accuranker_keywords_rank.highest_ranking_page_rank,
base_accuranker_keywords_rank.landing_page AS page,
CASE 
    WHEN base_accuranker_keywords_rank.highest_ranking_page = base_accuranker_keywords_rank.landing_page 
        THEN COALESCE(highest_ranking_page_rank,rank) 
    ELSE rank
END AS page_rank
FROM {{ ref('base_accuranker_keywords_rank')}} AS base_accuranker_keywords_rank
LEFT JOIN
{{ ref('base_accuranker_keywords')}} AS base_accuranker_keywords
ON base_accuranker_keywords.keyword = base_accuranker_keywords_rank.keyword
AND base_accuranker_keywords.date_period = base_accuranker_keywords_rank.date_period
AND base_accuranker_keywords.path = base_accuranker_keywords_rank.landing_page
WHERE extra_rank_page = ''),

extra_ranks AS (
SELECT 
DISTINCT
id_fk, 
keyword_id,
date_period,
keyword,
prefered_page,
landing_page,
highest_ranking_page,
highest_ranking_page_rank,
extra_rank_page AS page,
CAST (extra_rank_rank AS numeric) AS rank
FROM {{ ref('base_accuranker_keywords_rank')}} AS base_accuranker_keywords_rank
WHERE extra_rank_page != ''
),

highest_ranks AS (
SELECT 
DISTINCT
id_fk, 
keyword_id,
date_period,
keyword,
prefered_page,
landing_page,
highest_ranking_page,
highest_ranking_page_rank,
highest_ranking_page AS page,
CAST (highest_ranking_page_rank AS numeric) AS rank
FROM {{ ref('base_accuranker_keywords_rank')}} AS base_accuranker_keywords_rank
),

union_pages AS (
SELECT * FROM highest_ranks
UNION DISTINCT
SELECT * FROM landing_pages
UNION DISTINCT
SELECT * FROM extra_ranks
)

SELECT 
*,
CASE 
    WHEN prefered_page = page THEN TRUE
    ELSE FALSE
END AS prefered_page_ranking,
CASE 
    WHEN prefered_page = highest_ranking_page THEN TRUE
    ELSE FALSE
END AS prefered_page_is_highest_ranked_page,
CASE 
    WHEN page = highest_ranking_page THEN TRUE
    ELSE FALSE
END AS highest_ranking_page_ranking,
MAX(date_period) OVER (PARTITION by keyword_id) AS lastest_date_period,
CASE 
    WHEN date_period = MAX(date_period) OVER (PARTITION by keyword_id) THEN TRUE
    ELSE FALSE
END AS is_lastest_date_period
FROM union_pages
