{% if var('crm_warehouse_company_sources') %}

{{config(materialized="table")}}

with
companies_pre_merged AS (
SELECT
      *
    FROM {{ ref('int_companies_pre_merged') }}
),
merged AS (
SELECT c.company_name,
       case when m.company_name is not null then m.all_company_ids else c.all_company_ids end AS all_company_ids,
       c.company_phone,
       c.company_website,
       c.company_industry,
       c.company_linkedin_company_page,
       c.company_linkedin_bio,
       c.company_twitterhandle,
       c.company_description,
       c.company_finance_status,
       c.company_currency_code,
       c.company_created_date,
       c.company_last_modified_date,
       c.all_company_addresses
       FROM companies_pre_merged c

       {% if var("enable_companies_merge_file") %}

       {% if target.type == 'bigquery' %}

           left outer join (
                SELECT company_name,
                ARRAY(SELECT DISTINCT x
                        FROM UNNEST(all_company_ids) AS x) AS all_company_ids
                FROM (
                     SELECT company_name, array_concat_agg(all_company_ids) AS all_company_ids
                     FROM (
                          SELECT * FROM (
                              SELECT
                              c2.company_name AS company_name,
                              c2.all_company_ids AS all_company_ids
                              FROM   {{ ref('companies_merge_list') }} m
                              join companies_pre_merged c1 on m.old_company_id in UNNEST(c1.all_company_ids)
                              join companies_pre_merged c2 on m.company_id in UNNEST(c2.all_company_ids)
                              )
                          union all
                          SELECT * FROM (
                              SELECT
                              c2.company_name AS company_name,
                              c1.all_company_ids AS all_company_ids
                              FROM   {{ ref('companies_merge_list') }} m
                              join companies_pre_merged c1 on m.old_company_id in UNNEST(c1.all_company_ids)
                              join companies_pre_merged c2 on m.company_id in UNNEST(c2.all_company_ids)
                              )
                     )
                     group by 1
                )) m
           on c.company_name = m.company_name
           where c.company_name not in (
               SELECT
               c2.company_name
               FROM   {{ ref('companies_merge_list') }} m
               join companies_pre_merged c2 on m.old_company_id in UNNEST(c2.all_company_ids)
             )

         {% elif target.type == 'snowflake' %}

             left outer join (
                      SELECT company_name, array_agg(all_company_ids) AS all_company_ids
                           FROM (
                             SELECT
                               c2.company_name AS company_name,
                               c2.all_company_ids AS all_company_ids
                             FROM   {{ ref('companies_merge_list') }} m
                             join (
                               SELECT c1.company_name, c1f.value::string AS all_company_ids FROM {{ ref('int_companies_pre_merged') }} c1,table(flatten(c1.all_company_ids)) c1f) c1
                             on m.old_company_id = c1.all_company_ids
                             join (
                               SELECT c2.company_name, c2f.value::string AS all_company_ids FROM {{ ref('int_companies_pre_merged') }} c2,table(flatten(c2.all_company_ids)) c2f) c2
                             on m.company_id = c2.all_company_ids
                             union all
                             SELECT
                               c2.company_name AS company_name,
                               c1.all_company_ids AS all_company_ids
                             FROM   {{ ref('companies_merge_list') }} m
                             join (
                               SELECT c1.company_name, c1f.value::string AS all_company_ids FROM {{ ref('int_companies_pre_merged') }} c1,table(flatten(c1.all_company_ids)) c1f) c1
                               on m.old_company_id = c1.all_company_ids
                               join (
                                 SELECT c2.company_name, c2f.value::string AS all_company_ids FROM {{ ref('int_companies_pre_merged') }} c2,table(flatten(c2.all_company_ids)) c2f) c2
                               on m.company_id = c2.all_company_ids
                             )
                       group by 1
                  ) m
             on c.company_name = m.company_name
             where c.company_name not in (
                 SELECT
                 c2.company_name
                 FROM   {{ ref('companies_merge_list') }} m
                 join (SELECT c2.company_name, c2f.value::string AS all_company_ids
                       FROM {{ ref('int_companies_pre_merged') }} c2,table(flatten(c2.all_company_ids)) c2f) c2
                       on m.old_company_id = c2.all_company_ids)

           {% else %}
               {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}

           {% endif %}

       {% endif %}

       )
SELECT * FROM merged

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
