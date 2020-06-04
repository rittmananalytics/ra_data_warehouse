## Dimension Merge and Deduplication Across Multiple Data Sources

Customers, contacts, projects and other shared dimensions are automatically created from all data sources, deduplicating by name and merge lookup files using a process that preserves source system keys whilst assigning a unique ID for each customer, contact etc.

### Design Pattern

1. Each set of source adapter dbt dimension table models provides a unique ID, prefixed with the source name, and another field value (for example, user name) that can be used for deduplicating dimension members downstream. These fields are then initially merged (UNION ALL) together in the i_* integration view.

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/merge.png)

2. An CTE containing an array of source dimension IDs is then created within the int_ integration view, grouped by the deduplication column (in this example, user name)

```
user_emails as (
       SELECT user_name, 
	      array_agg(distinct lower(user_email) ignore nulls) as all_user_emails
       FROM   t_users_merge_list
       GROUP BY 1),

```
Any other multivalue columns are similarly-grouped by the deduplication column in further CTEs within the i_ integration view, for example list of email addresses for a user.

```
user_ids as (
       SELECT user_name, 
              array_agg(user_id ignore nulls) as all_user_ids
       FROM   t_users_merge_list
       GROUP BY 1)

```

For dimensions where merging of members by name is not sufficient (for example, company names that cannot be relied on to always be spelt the same across all sources) we can add seed files to map one member to another and then extend the logic of the merge to make use of this merge file, for example:

```
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
3. Within the i_ integration view, all remaining columns are then deduplicated by the deduplication column.

```
SELECT user_name,
		MAX(user_is_contractor) as user_is_contractor,
		MAX(user_is_staff) as user_is_staff,
		MAX(user_weekly_capacity) as user_weekly_capacity ,
		MAX(user_phone) as user_phone,
		MAX(user_default_hourly_rate) as user_default_hourly_rate,
		MAX(user_cost_rate) as user_cost_rate,
		MAX(user_is_active) as user_is_active,
		MAX(user_created_ts) as user_created_ts,
		MAX(user_last_modified_ts) as user_last_modified_ts,
	FROM t_users_merge_list
	GROUP BY 1
```

4. Then this deduplicated CTE is joined-back to the CTE, along with any other multivalue column CTEs

```
SELECT i.all_user_ids,
        u.*,
        e.all_user_emails
 FROM (
	SELECT user_name,
		MAX(user_is_contractor) as user_is_contractor,
		MAX(user_is_staff) as user_is_staff,
		MAX(user_weekly_capacity) as user_weekly_capacity ,
		MAX(user_phone) as user_phone,
		MAX(user_default_hourly_rate) as user_default_hourly_rate,
		MAX(user_cost_rate) as user_cost_rate,
		MAX(user_is_active) as user_is_active,
		MAX(user_created_ts) as user_created_ts,
		MAX(user_last_modified_ts) as user_last_modified_ts,
	FROM t_users_merge_list
	GROUP BY 1) u
JOIN user_emails e 
ON u.user_name = COALESCE(e.user_name,'Unknown')
JOIN user_ids i 
ON u.user_name = i.user_name
```

5. The wh_ warehouse dimension table then adds a unique GUID for each dimension member as a surrogate key.

```
WITH users AS
  (
  SELECT * from {{ ref('int_users') }}
)
select GENERATE_UUID() as user_pk,
       u.*
from users u
```

6. The i_ integration view for the associated fact table contains rows referencing these deduplicated dimension members using the source system IDs e.g. 'harvest-2122', 'asana-22122'

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/fact_source_integration.png)

7. When loading the associated wh_ fact table, the lookup to the wh_ dimension table uses UNNEST() to query the array of source system IDs, returning the wh_ dimension GUID as the dimension surrogate key

```
with user_dim as (
    select *
    from {{ ref('wh_users_dim') }}
),
  timesheets as (
      select *
      from {{ ref('int_timesheets') }}
)
SELECT

    GENERATE_UUID() as timesheet_pk,
    s.user_pk,
    timesheet_billable_hourly_rate_amount,
    timesheet_billable_hourly_cost_amountFROM
   timesheets t
JOIN user_dim s
   ON cast(t.timesheet_users_id as string) IN UNNEST(s.all_user_ids)
```

8. The wh_ dimension table contains the source system IDs and other multivalue dimension columns as BigQuery repeating columns

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/dimension_table_with_multivalue_source_ids_and_other_columns.png)

9. The wh_ fact table contains dimension foreign keys in the form of these GUIDs, as opposed to the source IDs for the dimensions being joined to.

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/fact_table_with_dimension_guids.png)
