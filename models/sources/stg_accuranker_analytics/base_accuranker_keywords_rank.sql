WITH source AS (
SELECT
*,
row_number() over (PARTITION BY id order by date_period) AS row_number
FROM `client-enux`.`apiaccuranker`.`keyword_path_ranks`
),

source_cleaned AS (
SELECT 
id, 
id_fk,
keyword_id,
TIMESTAMP_MILLIS(date_period) AS date_period,
keyword, 
created_at,
CONCAT("www.fluentu.com",prefered_path) AS prefered_page,
prefered_path,
REGEXP_EXTRACT(CONCAT("www.fluentu.com",landing_page_path),r'^([^\?]*)\??')  AS landing_page,
landing_page_path,
REGEXP_REPLACE((SPLIT(highest_ranking_page,'#:~:')[SAFE_OFFSET(0)]),r'^[Hh]ttps?:\/?\/?','') AS highest_ranking_page,
highest_ranking_page_rank,
REGEXP_REPLACE(extra_rank_page,r'^[Hh]ttps?:\/?\/?','') AS extra_rank_page,
extra_rank_rank
FROM source
WHERE row_number = 1
),

extra_ranks AS (
SELECT 
date_period,
keyword,
prefered_page,
extra_rank_page,
extra_rank_rank
FROM source_cleaned
WHERE extra_rank_page != ''
),

prefered_page_ranks AS (
SELECT 
date_period,
keyword,
prefered_page,
extra_rank_page,
extra_rank_rank AS prefered_page_rank
FROM source_cleaned
WHERE extra_rank_page != ''
AND extra_rank_page = prefered_page
),

landing_page_ranks AS (
SELECT 
date_period,
keyword,
landing_page,
extra_rank_page,
extra_rank_rank AS landing_page_rank
FROM source_cleaned
WHERE extra_rank_page != ''
AND extra_rank_page = landing_page
)

SELECT 
source_cleaned.*,
prefered_page_rank,
landing_page_rank
FROM source_cleaned
LEFT JOIN 
prefered_page_ranks
ON source_cleaned.date_period = prefered_page_ranks.date_period
AND source_cleaned.keyword = prefered_page_ranks.keyword
AND source_cleaned.prefered_page = prefered_page_ranks.prefered_page
LEFT JOIN 
landing_page_ranks
ON source_cleaned.date_period = landing_page_ranks.date_period
AND source_cleaned.keyword = landing_page_ranks.keyword
AND source_cleaned.landing_page = landing_page_ranks.landing_page


