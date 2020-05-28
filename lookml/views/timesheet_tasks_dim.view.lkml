view: timesheet_tasks_dim {
  sql_table_name: `timesheet_tasks_dim`
    ;;

  dimension: task_billable_by_default {
    hidden: yes

    type: yesno
    sql: ${TABLE}.task_billable_by_default ;;
  }

  dimension_group: task_created {
    hidden: yes

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
    sql: ${TABLE}.task_created_at ;;
  }

  dimension: task_default_hourly_rate {
    hidden: yes

    type: number
    sql: ${TABLE}.task_default_hourly_rate ;;
  }

  dimension: task_id {
    hidden: yes

    type: number
    sql: ${TABLE}.task_id ;;
  }

  dimension: task_is_active {
    hidden: yes

    type: yesno
    sql: ${TABLE}.task_is_active ;;
  }

  dimension: task_name {
    type: string
    sql: ${TABLE}.task_name ;;
  }

  dimension_group: task_updated {
    hidden: yes
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
    sql: ${TABLE}.task_updated_at ;;
  }

  dimension: timesheet_task_pk {
    primary_key: yes
    type: string
    sql: ${TABLE}.timesheet_task_pk ;;
  }

  measure: count {
    hidden: yes

    type: count
    drill_fields: [task_name]
  }
}