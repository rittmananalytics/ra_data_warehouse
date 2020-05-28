view: delivery_tasks_dim {
  sql_table_name: `delivery_tasks_dim`
    ;;

  dimension: project_id {
    type: string
    sql: ${TABLE}.project_id ;;
  }

  dimension_group: task_completed_ts {
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
    sql: ${TABLE}.task_completed_ts ;;
  }

  dimension_group: task_created_ts {
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
    sql: ${TABLE}.task_created_ts ;;
  }

  dimension: task_creator_user_id {
    type: string
    sql: ${TABLE}.task_creator_user_id ;;
  }

  dimension: task_description {
    type: string
    sql: ${TABLE}.task_description ;;
  }

  dimension: task_id {
    type: string
    sql: ${TABLE}.task_id ;;
  }

  dimension: task_is_completed {
    type: yesno
    sql: ${TABLE}.task_is_completed ;;
  }

  dimension_group: task_last_modified_ts {
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
    sql: ${TABLE}.task_last_modified_ts ;;
  }

  dimension: task_name {
    type: string
    sql: ${TABLE}.task_name ;;
  }

  dimension: task_pk {
    type: string
    sql: ${TABLE}.task_pk ;;
  }

  dimension: task_priority {
    type: string
    sql: ${TABLE}.task_priority ;;
  }

  dimension: task_status {
    type: string
    sql: ${TABLE}.task_status ;;
  }

  dimension: task_type {
    type: string
    sql: ${TABLE}.task_type ;;
  }

  measure: count {
    type: count
    drill_fields: [task_name]
  }
}