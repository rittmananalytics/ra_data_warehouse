{{ config(
  enabled = target.type == 'snowflake'
) }}

{% if var("projects_warehouse_delivery_sources") %}
  {% if 'jira_projects' in var("projects_warehouse_delivery_sources") %}
    WITH source AS (

      SELECT
        *
      FROM
        (
          SELECT
            CONCAT(
              '{{ var(' stg_jira_projects_id - prefix ') }}',
              id
            ) AS project_id,
            CONCAT(
              '{{ var(' stg_jira_projects_id - prefix ') }}',
              REPLACE(
                NAME,
                ' ',
                '_'
              )
            ) AS company_id,
            CONCAT(
              '{{ var(' stg_jira_projects_id - prefix ') }}',
              LEAD :accountId :: STRING
            ) AS lead_user_id,
            NAME AS project_name,
            projectkeys.value :: STRING AS projectkeys,
            projecttypekey AS project_type_id,
            CAST (
              NULL AS STRING
            ) AS project_status,
            CAST (
              NULL AS STRING
            ) AS project_notes,
            projectcategory :id :: STRING AS project_category_id,
            _sdc_batched_at,
            MAX(_sdc_batched_at) over (
              PARTITION BY id
              ORDER BY
                _sdc_batched_at RANGE BETWEEN unbounded preceding
                AND unbounded following
            ) AS max_sdc_batched_at
          FROM
            {{ var('stg_jira_projects_stitch_projects_table') }},
            TABLE(FLATTEN(projectkeys)) projectkeys)
          WHERE
            _sdc_batched_at = max_sdc_batched_at
        ),
        types AS (
          SELECT
            *
          FROM
            (
              SELECT
                key AS project_type_id,
                formattedKey AS project_type,
                _sdc_batched_at,
                MAX(_sdc_batched_at) over (
                  PARTITION BY key
                  ORDER BY
                    _sdc_batched_at RANGE BETWEEN unbounded preceding
                    AND unbounded following
                ) AS max_sdc_batched_at
              FROM
                {{ var('stg_jira_projects_stitch_project_types_table') }}
            )
          WHERE
            _sdc_batched_at = max_sdc_batched_at
        ),
        categories AS (
          SELECT
            *
          FROM
            (
              SELECT
                id AS project_category_id,
                description AS project_category_description,
                NAME AS project_category_name,
                _sdc_batched_at,
                MAX(_sdc_batched_at) over (
                  PARTITION BY id
                  ORDER BY
                    _sdc_batched_at RANGE BETWEEN unbounded preceding
                    AND unbounded following
                ) AS max_sdc_batched_at
              FROM
                {{ var('stg_jira_projects_stitch_project_categories_table') }}
            )
          WHERE
            _sdc_batched_at = max_sdc_batched_at
        )
      SELECT
        p.project_id,
        p.lead_user_id,
        p.company_id,
        p.project_name,
        p.project_status,
        p.project_notes,
        t.project_type AS project_type,
        C.project_category_description,
        C.project_category_name,
        CAST (
          NULL AS TIMESTAMP
        ) AS project_created_at_ts,
        CAST (
          NULL AS TIMESTAMP
        ) AS project_modified_at_ts
      FROM
        source p
        LEFT OUTER JOIN types t
        ON p.project_type_id = t.project_type_id
        LEFT OUTER JOIN categories C
        ON p.project_category_id = C.project_category_id
      {% else %}
        {{ config(
          enabled = false
        ) }}
      {% endif %}
      {% else %}
        {{ config(
          enabled = false
        ) }}
      {% endif %}
