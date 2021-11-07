{% if var('crm_warehouse_company_sources') %}

{{config(materialized="table")}}

with
companies_pre_merged as (
select
      *
    from {{ ref('int_companies_pre_merged') }}
),
merged as (
select c.company_name,
       case when m.company_name is not null then m.all_company_ids else c.all_company_ids end as all_company_ids,
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
       from companies_pre_merged c

       {% if var("enable_companies_merge_file") %}

       {% if target.type == 'bigquery' %}

           left outer join (
                select company_name,
                ARRAY(SELECT DISTINCT x
                        FROM UNNEST(all_company_ids) AS x) as all_company_ids
                from (
                     select company_name, array_concat_agg(all_company_ids) as all_company_ids
                     from (
                          select * from (
                              select
                              c2.company_name as company_name,
                              c2.all_company_ids as all_company_ids
                              from   {{ ref('companies_merge_list') }} m
                              join companies_pre_merged c1 on m.old_company_id in UNNEST(c1.all_company_ids)
                              join companies_pre_merged c2 on m.company_id in UNNEST(c2.all_company_ids)
                              )
                          union all
                          select * from (
                              select
                              c2.company_name as company_name,
                              c1.all_company_ids as all_company_ids
                              from   {{ ref('companies_merge_list') }} m
                              join companies_pre_merged c1 on m.old_company_id in UNNEST(c1.all_company_ids)
                              join companies_pre_merged c2 on m.company_id in UNNEST(c2.all_company_ids)
                              )
                     )
                     group by 1
                )) m
           on c.company_name = m.company_name
           where c.company_name not in (
               select
               c2.company_name
               from   {{ ref('companies_merge_list') }} m
               join companies_pre_merged c2 on m.old_company_id in UNNEST(c2.all_company_ids)
             )

         {% elif target.type == 'snowflake' %}

             left outer join (
                      select company_name, array_agg(all_company_ids) as all_company_ids
                           from (
                             select
                               c2.company_name as company_name,
                               c2.all_company_ids as all_company_ids
                             from   {{ ref('companies_merge_list') }} m
                             join (
                               SELECT c1.company_name, c1f.value::string as all_company_ids from {{ ref('int_companies_pre_merged') }} c1,table(flatten(c1.all_company_ids)) c1f) c1
                             on m.old_company_id = c1.all_company_ids
                             join (
                               SELECT c2.company_name, c2f.value::string as all_company_ids from {{ ref('int_companies_pre_merged') }} c2,table(flatten(c2.all_company_ids)) c2f) c2
                             on m.company_id = c2.all_company_ids
                             union all
                             select
                               c2.company_name as company_name,
                               c1.all_company_ids as all_company_ids
                             from   {{ ref('companies_merge_list') }} m
                             join (
                               SELECT c1.company_name, c1f.value::string as all_company_ids from {{ ref('int_companies_pre_merged') }} c1,table(flatten(c1.all_company_ids)) c1f) c1
                               on m.old_company_id = c1.all_company_ids
                               join (
                                 SELECT c2.company_name, c2f.value::string as all_company_ids from {{ ref('int_companies_pre_merged') }} c2,table(flatten(c2.all_company_ids)) c2f) c2
                               on m.company_id = c2.all_company_ids
                             )
                       group by 1
                  ) m
             on c.company_name = m.company_name
             where c.company_name not in (
                 select
                 c2.company_name
                 from   {{ ref('companies_merge_list') }} m
                 join (SELECT c2.company_name, c2f.value::string as all_company_ids
                       from {{ ref('int_companies_pre_merged') }} c2,table(flatten(c2.all_company_ids)) c2f) c2
                       on m.old_company_id = c2.all_company_ids)

           {% else %}
               {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}

           {% endif %}

       {% endif %}

       )
select * from merged

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
