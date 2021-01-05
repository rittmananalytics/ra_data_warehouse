{% if var('crm_warehouse_contact_sources') %}

{{config(materialized="table")}}

with t_contacts_merge_list as
  (
    {% for source in var('crm_warehouse_contact_sources') %}
      {% set relation_source = 'stg_' + source + '_contacts' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  ),

{% if target.type == 'bigquery' %}

    contact_emails as (
             SELECT contact_name, array_agg(distinct lower(contact_email) ignore nulls) as all_contact_emails
             FROM t_contacts_merge_list
             group by 1),
    contact_ids as (
             SELECT contact_name, array_agg(contact_id ignore nulls) as all_contact_ids
             FROM t_contacts_merge_list
             group by 1),
    contact_company_ids as (
                   SELECT contact_name, array_agg(contact_company_id ignore nulls) as all_contact_company_ids
                   FROM t_contacts_merge_list
                   group by 1),
    contact_company_addresses as (
             select contact_name, ARRAY_AGG(STRUCT( contact_address, contact_city, contact_state, contact_country, contact_postcode_zip)) as all_contact_addresses
             FROM t_contacts_merge_list
             group by 1),

{% elif target.type == 'snowflake' %}

    contact_emails as (
             SELECT contact_name, array_agg(distinct lower(contact_email)) as all_contact_emails
             FROM t_contacts_merge_list
             group by 1),
    contact_ids as (
             SELECT contact_name, array_agg(contact_id) as all_contact_ids
             FROM t_contacts_merge_list
             group by 1),
    contact_company_ids as (
                   SELECT contact_name, array_agg(contact_company_id) as all_contact_company_ids
                   FROM t_contacts_merge_list
                   group by 1),
    contact_company_addresses as (
             select contact_name,
                       array_agg(
                            parse_json (
                              concat('{"contact_address":"',contact_address,
                                     '", "contact_city":"',contact_city,
                                     '", "contact_state":"',contact_state,
                                     '", "contact_country":"',contact_country,
                                     '", "contact_postcode_zip":"',contact_postcode_zip,'"} ')
                            )
                       ) as all_contact_addresses
             FROM t_contacts_merge_list
             group by 1),

{% else %}
      {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}

{% endif %}

contacts as (
   select all_contact_ids,
          c.contact_name,
          job_title,
          contact_phone,
          contact_is_contractor,
          contact_is_staff,
          contact_weekly_capacity,
          contact_default_hourly_rate,
          contact_cost_rate,
          contact_is_active,
          contact_created_date,
          contact_last_modified_date,
          e.all_contact_emails,
          a.all_contact_addresses,
          cc.all_contact_company_ids
         from (
            select contact_name,
                max(contact_job_title) as job_title,
                max(contact_phone) as contact_phone,
                min(contact_created_date) as contact_created_date,
                max(contact_last_modified_date) as contact_last_modified_date,
                max(contact_is_contractor)         as contact_is_contractor,
                max(contact_is_staff) as contact_is_staff,
                max(contact_weekly_capacity)          as contact_weekly_capacity,
                max(contact_default_hourly_rate)          as contact_default_hourly_rate,
                max(contact_cost_rate)           as contact_cost_rate,
                max(contact_is_active)                          as contact_is_active
            FROM t_contacts_merge_list
         group by 1) c
  join contact_emails e on c.contact_name = e.contact_name
  join contact_ids i on c.contact_name = i.contact_name
  join contact_company_addresses a on c.contact_name = a.contact_name
  join contact_company_ids cc on c.contact_name = cc.contact_name)
select * from contacts

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
