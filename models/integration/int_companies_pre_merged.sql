
{% if var('crm_warehouse_company_sources') %}

{{config(materialized="table")}}

with t_companies_pre_merged AS (

    {% for source in var('crm_warehouse_company_sources') %}
      {% set relation_source = 'stg_' + source + '_companies' %}

      SELECT
        '{{source}}' AS source,
        *
        FROM {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}

    ),

{% if target.type == 'bigquery' %}

      all_company_ids AS (
             SELECT company_name, array_agg(distinct company_id ignore nulls) AS all_company_ids
             FROM t_companies_pre_merged
             group by 1),
      all_company_addresses AS (
             SELECT company_name, array_agg(struct(company_address,
                                                   company_address2,
                                                   company_city,
                                                   company_state,
                                                   company_country,
                                                   company_zip) ignore nulls) AS all_company_addresses
             FROM t_companies_pre_merged
             group by 1),

{% elif target.type == 'snowflake' %}

      all_company_ids AS (
          SELECT company_name,
                 array_agg(
                    distinct company_id
                  ) AS all_company_ids
            FROM t_companies_pre_merged
          group by 1),
      all_company_addresses AS (
          SELECT company_name,
                 array_agg(
                      parse_json (
                        CONCAT('{"company_address":"',company_address,
                               '", "company_address2":"',company_address2,
                               '", "company_city":"',company_city,
                               '", "company_state":"',company_state,
                               '", "company_country":"',company_country,
                               '", "company_zip":"',company_zip,'"} ')
                      )
                 ) AS all_company_addresses
          FROM t_companies_pre_merged
          where length(coalesce(company_address,company_address2,company_city,company_state,company_country,company_zip)) >0
          group by 1
      ),

{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}

{% endif %}

grouped AS (
      SELECT
      company_name,
      max(company_phone) AS company_phone,
      max(company_website) AS company_website,
      max(company_industry) AS company_industry,
      max(company_linkedin_company_page) AS company_linkedin_company_page,
      max(company_linkedin_bio) AS company_linkedin_bio,
      max(company_twitterhandle) AS company_twitterhandle,
      max(company_description) AS company_description,
      max(company_finance_status) AS company_finance_status,
      max(company_currency_code) AS company_currency_code,
      min(company_created_date) AS company_created_date,
      max(company_last_modified_date) AS company_last_modified_date
    FROM t_companies_pre_merged
      group by 1
),
joined AS (
      SELECT i.all_company_ids,
      g.*,
      a.all_company_addresses
      FROM grouped g
      JOIN all_company_ids i ON g.company_name = i.company_name
      LEFT OUTER JOIN all_company_addresses a ON g.company_name = a.company_name
)
SELECT * FROM joined

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
