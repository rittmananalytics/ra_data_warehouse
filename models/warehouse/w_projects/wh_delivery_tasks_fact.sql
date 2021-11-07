{% if var("projects_warehouse_delivery_sources") %}
{{
    config(
        unique_key='delivery_task_pk',
        alias='delivery_tasks_fact'
    )
}}


WITH tasks AS
  (
  SELECT {{ dbt_utils.star(from=ref('int_delivery_tasks')) }}
  FROM   {{ ref('int_delivery_tasks') }}
),
     projects AS
  (
    SELECT {{ dbt_utils.star(from=ref('wh_delivery_projects_dim')) }}
    FROM   {{ ref('wh_delivery_projects_dim') }}

  ),
  {% if target.type == 'bigquery' %}
    contacts_dim AS (
      SELECT
        {{ dbt_utils.star(
          from = ref('wh_contacts_dim')
        ) }}
      FROM
        {{ ref('wh_contacts_dim') }}
    )
    {% elif target.type == 'snowflake' %}
    contacts_dim as (
      SELECT
        c.contact_pk,
        cf.value::string as contact_id
      from {{ ref('wh_contacts_dim') }}
        c,table(flatten(c.all_contact_ids)) cf)
  {% else %}
    {{ exceptions.raise_compiler_error(
      target.type ~ " not supported in this project"
    ) }}
  {% endif %}
SELECT
  {{ dbt_utils.surrogate_key(['task_id']) }} as delivery_task_pk,
   p.delivery_project_pk,
   u.contact_pk,
   t.*
FROM
   tasks t
{% if target.type == 'bigquery' %}
JOIN
  contacts_dim u
ON
  CAST(
     t.task_assignee_user_id  AS STRING
   ) IN unnest(
     u.all_contact_ids
   )
{% elif target.type == 'snowflake' %}
JOIN
  contacts_dim u
ON
  t.task_assignee_user_id  :: STRING = u.contact_id
   {% else %}
{{ exceptions.raise_compiler_error(
     target.type ~ " not supported in this project"
   ) }}
{% endif %}
LEFT OUTER JOIN
  projects p
ON
  t.project_id = p.project_id
{% else %}

   {{config(enabled=false)}}

{% endif %}
