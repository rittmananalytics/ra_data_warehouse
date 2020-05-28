view: timesheets_fact {
  sql_table_name: `timesheets_fact`
    ;;

  dimension: company_pk {
    type: string
    sql: ${TABLE}.company_pk ;;
  }

  dimension: timesheet_billable_hourly_cost_amount {
    type: number
    sql: ${TABLE}.timesheet_billable_hourly_cost_amount ;;
  }

  measure: timesheet_avg_billable_hourly_cost_amount {
    type: average
    sql: ${TABLE}.timesheet_billable_hourly_cost_amount ;;
  }

  dimension: timesheet_billable_hourly_rate_amount {
    type: number
    sql: ${TABLE}.timesheet_billable_hourly_rate_amount ;;
  }

  measure: timesheet_avg_billable_hourly_rate_amount {
    type: average
    sql: ${TABLE}.timesheet_billable_hourly_rate_amount ;;
  }

  dimension_group: timesheet_billing {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timesheet_billing_date ;;
  }

  dimension: timesheet_has_been_billed {
    type: yesno
    sql: ${TABLE}.timesheet_has_been_billed ;;
  }

  dimension: timesheet_has_been_locked {
    type: yesno
    sql: ${TABLE}.timesheet_has_been_locked ;;
  }

  dimension: timesheet_hours_billed {
    type: number
    sql: ${TABLE}.timesheet_hours_billed ;;
  }

  measure: total_timesheet_hours_billed {
    type: sum_distinct
    sql: ${TABLE}.timesheet_hours_billed ;;
  }

  dimension: timesheet_invoice_id {
    type: number
    sql: ${TABLE}.timesheet_invoice_id ;;
  }

  dimension: timesheet_is_billable {
    type: yesno
    sql: ${TABLE}.timesheet_is_billable ;;
  }

  dimension: timesheet_notes {
    type: string
    sql: ${TABLE}.timesheet_notes ;;
  }

  dimension: timesheet_pk {
    type: string
    primary_key: yes
    sql: ${TABLE}.timesheet_pk ;;
  }

  dimension: timesheet_project_pk {
    type: string
    sql: ${TABLE}.timesheet_project_pk ;;
  }

  dimension: timesheet_task_pk {
    type: string
    sql: ${TABLE}.timesheet_task_pk ;;
  }

  dimension: timesheet_total_amount_billed {
    type: number
    sql: ${TABLE}.timesheet_total_amount_billed ;;
  }

  measure: total_timesheet_total_amount_billed {
    type: sum_distinct
    sql: ${TABLE}.timesheet_total_amount_billed ;;
  }

  dimension: user_pk {
    type: string
    sql: ${TABLE}.user_pk ;;
  }

  measure: total_consultants {
    type: count_distinct
    sql: ${TABLE}.user_pk ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
