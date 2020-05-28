view: timesheet_projects_dim {
  sql_table_name: `timesheet_projects_dim`
    ;;

  dimension: company_pk {
    type: string
    sql: ${TABLE}.company_pk ;;
  }

  dimension: project_budget_amount {
    type: number
    sql: ${TABLE}.project_budget_amount ;;
  }

  dimension: project_budget_by {
    type: string
    sql: ${TABLE}.project_budget_by ;;
  }

  dimension: project_code {
    type: string
    sql: ${TABLE}.project_code ;;
  }

  dimension: project_cost_budget {
    type: number
    sql: ${TABLE}.project_cost_budget ;;
  }

  dimension: project_delivery_end_ts {
    type: string
    sql: ${TABLE}.project_delivery_end_ts ;;
  }

  dimension: project_delivery_start_ts {
    type: string
    sql: ${TABLE}.project_delivery_start_ts ;;
  }

  dimension: project_fee_amount {
    type: number
    sql: ${TABLE}.project_fee_amount ;;
  }

  dimension: project_hourly_rate {
    type: number
    sql: ${TABLE}.project_hourly_rate ;;
  }

  dimension: project_is_active {
    type: yesno
    sql: ${TABLE}.project_is_active ;;
  }

  dimension: project_is_billable {
    type: yesno
    sql: ${TABLE}.project_is_billable ;;
  }

  dimension: project_is_expenses_included_in_cost_budget {
    type: yesno
    sql: ${TABLE}.project_is_expenses_included_in_cost_budget ;;
  }

  dimension: project_is_fixed_fee {
    type: yesno
    sql: ${TABLE}.project_is_fixed_fee ;;
  }

  dimension: project_name {
    type: string
    sql: ${TABLE}.project_name ;;
  }

  dimension: project_over_budget_notification_pct {
    type: number
    sql: ${TABLE}.project_over_budget_notification_pct ;;
  }

  dimension: timesheet_project_id {
    type: string
    sql: ${TABLE}.timesheet_project_id ;;
  }

  dimension: timesheet_project_pk {
    primary_key: yes
    type: string
    sql: ${TABLE}.timesheet_project_pk ;;
  }

  measure: count {
    type: count
    drill_fields: [project_name]
  }
}