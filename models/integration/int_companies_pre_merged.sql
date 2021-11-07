
{% if var('crm_warehouse_company_sources') %}

{{config(materialized="table")}}

with t_companies_pre_merged as (

    {% for source in var('crm_warehouse_company_sources') %}
      {% set relation_source = 'stg_' + source + '_companies' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}

    ),

{% if target.type == 'bigquery' %}

      all_company_ids as (
             SELECT company_name, array_agg(distinct company_id ignore nulls) as all_company_ids
             FROM t_companies_pre_merged
             group by 1),
      all_company_addresses as (
             SELECT company_name, array_agg(struct(company_address,
                                                   company_address2,
                                                   company_city,
                                                   company_state,
                                                   company_country,
                                                   company_zip) ignore nulls) as all_company_addresses
             FROM t_companies_pre_merged
             group by 1),

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
          FROM t_companies_pre_merged
          where length(coalesce(company_address,company_address2,company_city,company_state,company_country,company_zip)) >0
          group by 1
      ),

{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}

{% endif %}

grouped as (
      SELECT
      company_name,
      max(company_phone) as company_phone,
      max(company_website) as company_website,
      max(company_industry) as company_industry,
      max(company_linkedin_company_page) as company_linkedin_company_page,
      max(company_linkedin_bio) as company_linkedin_bio,
      max(company_twitterhandle) as company_twitterhandle,
      max(company_description) as company_description,
      max(company_finance_status) as company_finance_status,
      max(company_currency_code) as company_currency_code,
      min(company_created_date) as company_created_date,
      max(company_last_modified_date) as company_last_modified_date
    from t_companies_pre_merged
      group by 1
),
joined as (
      SELECT i.all_company_ids,
      g.*,
      a.all_company_addresses
      FROM grouped g
      JOIN all_company_ids i ON g.company_name = i.company_name
      LEFT OUTER JOIN all_company_addresses a ON g.company_name = a.company_name
)
select * from joined

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
