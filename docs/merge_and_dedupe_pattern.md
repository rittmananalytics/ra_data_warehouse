## Dimension Merge and Deduplication Across Multiple Data Sources

Customers, contacts, projects and other shared dimensions are automatically created from all data sources, deduplicating by name and merge lookup files using a process that preserves source system keys whilst assigning a unique ID for each customer, contact etc.

### Design Pattern

1. Each set of source adapter dbt dimension table models provides a unique ID, prefixed with the source name, and another field value (for example, user name) that can be used for deduplicating dimension members downstream. 

```
WITH source AS (
    {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_companies_table'),unique_column='companyid') }}
  ),
  renamed AS (
    SELECT
      CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',companyid) AS company_id,
      REPLACE(REPLACE(REPLACE(properties.name.value, 'Limited', ''), 'ltd', ''),', Inc.','') AS company_name,
      properties.address.value AS                   company_address,
      properties.address2.value AS                  company_address2,
      properties.city.value AS                      company_city,
      properties.state.value AS                     company_state,
      properties.country.value AS                   company_country,
      properties.zip.value AS                       company_zip,
      properties.phone.value AS                     company_phone,
      properties.website.value AS                   company_website,
      properties.industry.value AS                  company_industry,
      properties.linkedin_company_page.value AS     company_linkedin_company_page,
      properties.linkedinbio.value AS               company_linkedin_bio,
      properties.twitterhandle.value AS             company_twitterhandle,
      properties.description.value AS               company_description,
      CAST (NULL AS STRING) AS                      company_finance_status,
      cast (null as string)      as                 company_currency_code,
      properties.createdate.value AS                company_created_date,
      properties.hs_lastmodifieddate.value          company_last_modified_date
    FROM
      source
  )
SELECT
  *
FROM
  renamed
```

2. These tables are then initially merged (UNION ALL) together in the i_* integration view.

```
with t_companies_pre_merged as (

    {% for source in var('crm_warehouse_company_sources') %}
      {% set relation_source = 'stg_' + source + '_companies' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}

    )
```

3. An CTE containing an array of source dimension IDs is then created within the int_ integration view, grouped by the deduplication column (in this example, user name)

```
{% if target.type == 'bigquery' %}

  all_company_ids as (
             SELECT company_name, array_agg(distinct company_id ignore nulls) as all_company_ids
             FROM t_companies_pre_merged
             group by 1),
```

Any other multivalue columns are similarly-grouped by the deduplication column in further CTEs within the i_ integration view, for example list of email addresses for a user.

```
all_company_addresses as (
             SELECT company_name, array_agg(struct(company_address,
                                                   company_address2,
                                                   case when length(trim(company_city)) = 0 then null else company_city end as company_city,
                                                   case when length(trim(company_state)) = 0 then null else company_state end as company_state,
                                                   case when length(trim(company_country)) = 0 then null else company_country end as company_country,
                                                   case when length(trim(company_zip)) = 0 then null else company_zip  end as company_zip) ignore nulls) as all_company_addresses

```

If the target warehouse is Snowflake rather than BigQuery, this is detected through the target.type Jinja function and the functionally-equivalent Snowflake SQL version is used instead.

```
{% elif target.type == 'snowflake' %}

      all_company_ids as (
          SELECT company_name,
                 array_agg(
                    distinct company_id
                  ) as all_company_ids
            FROM t_companies_pre_merged
          group by 1),
      all_company_addresses as (
          SELECT company_name,
                 array_agg(
                      parse_json (
                        concat('{"company_address":"',company_address,
                               '", "company_address2":"',company_address2,
                               '", "company_city":"',company_city,
                               '", "company_state":"',company_state,
                               '", "company_country":"',company_country,
                               '", "company_zip":"',company_zip,'"} ')
                      )
                 ) as all_company_addresses

```

For dimensions where merging of members by name is not sufficient (for example, company names that cannot be relied on to always be spelt the same across all sources) we can add seed files to map one member to another and then extend the logic of the merge to make use of this merge file, for example when BigQuery is the target warehouse:

```
{% if target.type == 'bigquery' %}

from companies_pre_merged c
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
         ))
```

and if the target is Snowflake the following SQL is executed instead:

```
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
```

4. Within the i_ integration view, all remaining columns are then deduplicated by the deduplication column.

```
SELECT user_name,
		MAX(contact_is_contractor) as contact_is_contractor,
		MAX(contact_is_staff) as contact_is_staff,
		MAX(contact_weekly_capacity) as contact_weekly_capacity ,
		MAX(user_phone) as user_phone,
		MAX(contact_default_hourly_rate) as contact_default_hourly_rate,
		MAX(contact_cost_rate) as contact_cost_rate,
		MAX(contact_is_active) as contact_is_active,
		MAX(user_created_ts) as user_created_ts,
		MAX(user_last_modified_ts) as user_last_modified_ts,
	FROM t_users_merge_list
	GROUP BY 1
```

5. Then this deduplicated CTE is joined-back to the CTE, along with any other multivalue column CTEs

```
SELECT i.all_user_ids,
        u.*,
        e.all_user_emails
 FROM (
	SELECT user_name,
		MAX(contact_is_contractor) as contact_is_contractor,
		MAX(contact_is_staff) as contact_is_staff,
		MAX(contact_weekly_capacity) as contact_weekly_capacity ,
		MAX(user_phone) as user_phone,
		MAX(contact_default_hourly_rate) as contact_default_hourly_rate,
		MAX(contact_cost_rate) as contact_cost_rate,
		MAX(contact_is_active) as contact_is_active,
		MAX(user_created_ts) as user_created_ts,
		MAX(user_last_modified_ts) as user_last_modified_ts,
	FROM t_users_merge_list
	GROUP BY 1) u
JOIN user_emails e 
ON u.user_name = COALESCE(e.user_name,'Unknown')
JOIN user_ids i 
ON u.user_name = i.user_name
```

5. The wh_ warehouse dimension table then adds a surrogate key for each dimension member.

```
WITH companies_dim as (
  SELECT
    {{ dbt_utils.surrogate_key(['company_name']) }} as company_pk,
    *
  FROM
    {{ ref('int_companies') }} c
)
select * from companies_dim
```

6. The i_ integration view for the associated fact table contains rows referencing these deduplicated dimension members using the source system IDs e.g. 'harvest-2122', 'asana-22122'

7. When loading the associated wh_ fact table, the lookup to the wh_ dimension table uses UNNEST() to query the array of source system IDs when the target is BigQuery, returning the company_pk as the dimension surrogate key

```
WITH delivery_projects AS
  (
  SELECT *
  FROM   {{ ref('int_delivery_projects') }}
),
{% if target.type == 'bigquery' %}
  companies_dim as (
    SELECT {{ dbt_utils.star(from=ref('wh_companies_dim')) }}
    from {{ ref('wh_companies_dim') }}
  )
SELECT
	...
FROM
   delivery_projects p
   {% if target.type == 'bigquery' %}
     JOIN companies_dim c
       ON p.company_id IN UNNEST(c.all_company_ids)
```

Wheras when Snowflake is the target warehouse, the following SQL is used instead:

```
{% elif target.type == 'snowflake' %}
companies_dim as (
    SELECT c.company_pk, cf.value::string as company_id
    from {{ ref('wh_companies_dim') }} c,table(flatten(c.all_company_ids)) cf
)
SELECT 
   ...
FROM
   delivery_projects p
{% elif target.type == 'snowflake' %}
     JOIN companies_dim c
       ON p.company_id = c.company_id
```

8. The wh_ dimension table contains the source system IDs and other multivalue dimension columns as repeating columns for BigQuery warehouse targets and Variant datatypes containing JSON values for Snowflake.
