--To disable this model, set the shopify__using_refund variable within your dbt_project.yml file to False.
{{ config(enabled=var('shopify__using_refund', True)) }}

{{
    fivetran_utils.union_data(
        table_identifier='refund', 
        database_variable='shopify_database', 
        schema_variable='shopify_schema', 
        default_database=target.database,
        default_schema='shopify',
        default_variable='refund_source'
    )
}}