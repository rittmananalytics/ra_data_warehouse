{{ config(enabled = target.type == 'snowflake') }} { %

IF var("crm_warehouse_contact_sources") % } { %
	IF 'jira_projects' IN var("crm_warehouse_contact_sources") % } WITH source AS ({{ filter_stitch_relation(relation = var('stg_jira_projects_stitch_users_table'), unique_column = 'accountid') }}),
		renamed AS (
			SELECT CONCAT (
					'{{ var(' stg_jira_projects_id - prefix ') }}',
					accountid
					) AS contact_id,
				{{ dbt_utils.split_part('displayname', "' '", 1) }} AS contact_first_name,
				{{ dbt_utils.split_part('displayname', "' '", 2) }} AS contact_last_name,
				displayname AS contact_name,
				CAST(NULL AS STRING) AS contact_job_title,
				emailaddress AS contact_email,
				CAST(NULL AS STRING) AS contact_phone,
				CAST(NULL AS STRING) AS contact_mobile_phone,
				CAST(NULL AS STRING) AS contact_address,
				CAST(NULL AS STRING) AS contact_city,
				CAST(NULL AS STRING) AS contact_state,
				CAST(NULL AS STRING) AS contact_country,
				CAST(NULL AS STRING) AS contact_postcode_zip,
				CAST(NULL AS STRING) AS contact_company,
				CAST(NULL AS STRING) AS contact_website,
				CAST(NULL AS STRING) AS contact_company_id,
				CAST(NULL AS STRING) AS contact_owner_id,
				CAST(NULL AS STRING) AS contact_lifecycle_stage,
				CAST(NULL AS BOOLEAN) AS contact_is_contractor,
				CASE
					WHEN emailaddress LIKE '%@{{ var(' stg_jira_projects_staff_email_domain ') }}%'
						THEN TRUE
					ELSE FALSE
					END AS contact_is_staff,
				CAST(NULL AS INT) AS contact_weekly_capacity,
				CAST(NULL AS INT) AS contact_default_hourly_rate,
				CAST(NULL AS INT) AS contact_cost_rate,
				active AS contact_is_active,
				CAST(NULL AS TIMESTAMP) AS contact_created_date,
				CAST(NULL AS TIMESTAMP) AS contact_last_modified_date
			FROM source
			WHERE CONCAT (
					'{{ var(' stg_jira_projects_id - prefix ') }}',
					accountid
					) NOT LIKE '%addon%'

			UNION ALL

			SELECT CONCAT (
					'{{ var(' stg_jira_projects_id - prefix ') }}',
					- 999
					) AS contact_id,
				CAST(NULL AS STRING) AS contact_first_name,
				CAST(NULL AS STRING) AS contact_last_name,
				'Unassigned' AS contact_name,
				CAST(NULL AS STRING) AS contact_job_title,
				'unassigned@example.com' AS contact_email,
				CAST(NULL AS STRING) AS contact_phone,
				CAST(NULL AS STRING) AS contact_mobile_phone,
				CAST(NULL AS STRING) AS contact_address,
				CAST(NULL AS STRING) AS contact_city,
				CAST(NULL AS STRING) AS contact_state,
				CAST(NULL AS STRING) AS contact_country,
				CAST(NULL AS STRING) AS contact_postcode_zip,
				CAST(NULL AS STRING) AS contact_company,
				CAST(NULL AS STRING) AS contact_website,
				CAST(NULL AS STRING) AS contact_company_id,
				CAST(NULL AS STRING) AS contact_owner_id,
				CAST(NULL AS STRING) AS contact_lifecycle_stage,
				CAST(NULL AS BOOLEAN) AS user_is_contractor,
				FALSE AS user_is_staff,
				CAST(NULL AS INT) AS user_weekly_capacity,
				CAST(NULL AS INT) AS user_default_hourly_rate,
				CAST(NULL AS INT) AS user_cost_rate,
				FALSE AS user_is_active,
				CAST(NULL AS TIMESTAMP) AS contact_created_date,
				CAST(NULL AS TIMESTAMP) AS contact_last_modified_date
			)
		SELECT *
		FROM renamed { %
	ELSE
		% } {{ config(enabled = false) }} { % endif % } { %
ELSE
	% } {{ config(enabled = false) }} { % endif % }
