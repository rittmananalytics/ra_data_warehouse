WITH source AS (
SELECT
id,
keyword,
date_period,
tags,
row_number() over (PARTITION BY id order by date_period) AS row_number
from `client-enux`.`apiaccuranker`.`keywords`),

tags_nested AS (
SELECT 
id,
keyword,
date_period,
tags
FROM source
WHERE row_number = 1),

tags_unnested AS (
SELECT 
id,
keyword,
TIMESTAMP_MILLIS(date_period) as date_period, 
tags,
value AS tags_unested
FROM tags_nested, UNNEST(tags) AS tags)

SELECT
md5(id||tags_unested) AS pk,
id,
keyword,
date_period, 
tags_unested AS tag
FROM tags_unnested
