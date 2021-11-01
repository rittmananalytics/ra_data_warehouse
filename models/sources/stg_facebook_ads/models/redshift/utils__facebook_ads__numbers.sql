{{config(enabled = target.type == 'redshift')}}
{{ dbt_utils.generate_series(upper_bound=1000) }}
