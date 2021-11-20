{{config(enabled = target.type == 'bigquery')}}
SELECT * FROM (
SELECT *,
      (lag(row_count,1) over (PARTITION BYobject order by load_ts)) previous_load_ts,
      row_count-(lag(row_count,1) over (PARTITION BYobject order by load_ts)) AS diff_from_previous_load_ts,
      round( {{safe_divide('row_count-(lag(row_count,1) over (PARTITION BYobject order by load_ts))','(lag(row_count,1) over (PARTITION BYobject order by load_ts))') }},4) AS pct_diff_from_previous_load_ts,
      max(load_ts) over (PARTITION BYobject order by load_ts range between unbounded preceding and unbounded following) AS max_load_ts
      FROM {{target.project}}.{{ generate_prefixed_target_name() }}_logs.audit_dbt_results)
where load_ts = max_load_ts
and status != 'CREATE VIEW'
