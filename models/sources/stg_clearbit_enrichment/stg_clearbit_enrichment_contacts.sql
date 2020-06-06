{% if not var("enable_clearbit_enrichment_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  SELECT
   *
  FROM
   {{ target.database}}.{{ var('clearbit_schema') }}.{{ var('clearbit_contacts_table') }}
),
renamed as (
  SELECT
  concat('{{ var('id-prefix') }}',person__email) as contact_enrichment_id,
  person__email as contact_enrichment_email,
  person__name__fullName as contact_enrichment_full_name,
  person__name__givenName as contact_enrichment_given_name,
  person__name__familyName as contact_enrichment_family_name,
  person__location as contact_enrichment_location,
  person__timeZone as contact_enrichment_time_zone,
  person__utcOffset as contact_enrichment_utc_offset,
  person__geo__city as contact_enrichment_city,
  person__geo__state as contact_enrichment_state,
  person__geo__stateCode as contact_enrichment_state_code,
  person__geo__country as contact_enrichment_country,
  person__geo__countryCode as contact_enrichment_country_code,
  person__geo__lat as contact_enrichment_geo_lat,
  person__geo__lng as contact_enrichment_geo_long,
  person__bio as contact_enrichment_bio,
  person__site as contact_enrichment_website_url,
  person__employment__domain as contact_enrichment_employment_website_domain,
  person__employment__name as contact_enrichment_company_name,
  person__employment__title as contact_enrichment_title,
  person__employment__role as contact_enrichment_role,
  person__employment__subRole as contact_enrichment_sub_role,
  person__employment__seniority as contact_enrichment_role_seniority,
  person__facebook__handle as contact_enrichment_facebook_user_name,
  person__github__handle as contact_enrichment_github_user_name,
  person__github__id as contact_enrichment_github_id,
  person__github__company as contact_enrichment_github_company_name,
  person__github__followers as contact_enrichment_github_total_followers,
  person__github__following as contact_enrichment_github_total_following,
  person__twitter__handle as contact_enrichment_twitter_user_name,
  person__twitter__id as contact_enrichment_twitter_user_id,
  person__twitter__bio as contact_enrichment_twitter_bio,
  person__twitter__followers as contact_enrichment_twitter_total_followers,
  person__twitter__following as contact_enrichment_twitter_total_following,
  person__twitter__statuses as contact_enrichment_twitter_total_posts,
  person__twitter__favorites as contact_enrichment_twitter_total_favourites,
  person__twitter__location as contact_enrichment_twitter_location,
  person__twitter__site as contact_enrichment_twitter_website_url,
  person__linkedin__handle as contact_enrichment_linkedin_user_name,
  coalesce(person__indexedAt,current_timestamp()) as contact_enrichment_created_at,
  coalesce(person__indexedAt,current_timestamp()) as contact_enrichment_last_modified_at
FROM
  source
WHERE
  length(ltrim(person__email)) >0
)
select * from renamed
