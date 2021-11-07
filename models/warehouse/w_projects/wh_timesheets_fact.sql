{% if var("projects_warehouse_timesheet_sources") %}
  {{ config(
    unique_key = 'timesheet_projects_pk',
    alias = 'timesheets_fact'
  ) }}

  WITH {% if target.type == 'bigquery' %}
    companies_dim AS (

      SELECT
        {{ dbt_utils.star(
          from = ref('wh_companies_dim')
        ) }}
      FROM
        {{ ref('wh_companies_dim') }}
    ),
    contacts_dim AS (
      SELECT
        {{ dbt_utils.star(
          from = ref('wh_contacts_dim')
        ) }}
      FROM
        {{ ref('wh_contacts_dim') }}
    ),
    {% elif target.type == 'snowflake' %}
    companies_dim AS (
      SELECT
        C.company_pk,
        cf.value :: STRING AS company_id
      FROM
        {{ ref('wh_companies_dim') }} C,
        TABLE(FLATTEN(C.all_company_ids)) cf
    ),
contacts_dim as (
    SELECT c.contact_pk, cf.value::string as contact_id
    from {{ ref('wh_contacts_dim') }} c,table(flatten(c.all_contact_ids)) cf),

  {% else %}
    {{ exceptions.raise_compiler_error(
      target.type ~ " not supported in this project"
    ) }}
  {% endif %}
  tasks_dim AS (
    SELECT
      {{ dbt_utils.star(
        from = ref('wh_timesheet_tasks_dim')
      ) }}
    FROM
      {{ ref('wh_timesheet_tasks_dim') }}
  ),
  projects_dim AS (
    SELECT
      {{ dbt_utils.star(
        from = ref('wh_timesheet_projects_dim')
      ) }}
    FROM
      {{ ref('wh_timesheet_projects_dim') }}
  ),
  timesheets AS (
    SELECT
      {{ dbt_utils.star(
        from = ref('int_timesheets')
      ) }}
    FROM
      {{ ref('int_timesheets') }}
  )
SELECT
{{ dbt_utils.surrogate_key(['timesheet_id']) }} as timesheet_pk,
  C.company_pk,
  u.contact_pk,
  p.timesheet_project_pk,
  ta.timesheet_task_pk,
  timesheet_invoice_id,
  timesheet_billing_date,
  min(timesheet_billing_date) over (partition by c.company_pk order by timesheet_billing_date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_company_timesheet_billing_date,
  max(timesheet_billing_date) over (partition by c.company_pk order by timesheet_billing_date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_company_timesheet_billing_date,
  timesheet_hours_billed,
  timesheet_total_amount_billed,
  timesheet_is_billable,
  timesheet_has_been_billed,
  timesheet_has_been_locked,
  timesheet_billable_hourly_rate_amount,
  timesheet_billable_hourly_cost_amount,
  timesheet_notes
FROM
  timesheets t

  {% if target.type == 'bigquery' %}
    JOIN companies_dim C
    ON t.company_id IN unnest(
      C.all_company_ids
    )
    JOIN contacts_dim u
    ON CAST(
      t.timesheet_users_id AS STRING
    ) IN unnest(
      u.all_contact_ids
    )
  {% elif target.type == 'snowflake' %}
    JOIN companies_dim C
    ON t.company_id = C.company_id
    JOIN contacts_dim u
    ON t.timesheet_users_id :: STRING = u.contact_id
{% else %}
  {{ exceptions.raise_compiler_error(
    target.type ~ " not supported in this project"
  ) }}
{% endif %}
LEFT OUTER JOIN projects_dim p
ON t.timesheet_project_id = p.timesheet_project_id
LEFT OUTER JOIN tasks_dim ta
ON t.timesheet_task_id = ta.task_id
{% else %}
  {{ config(
    enabled = false
  ) }}
{% endif %}
