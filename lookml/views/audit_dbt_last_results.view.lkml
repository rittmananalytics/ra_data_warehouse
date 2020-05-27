view: audit_dbt_last_results {
  sql_table_name: `ra-development.analytics_logs.audit_dbt_last_results`
    ;;

  dimension: diff_from_previous_load_ts {
    type: number
    sql: ${TABLE}.diff_from_previous_load_ts ;;
  }

  measure: avg_diff_from_previous_load_ts {
    type: average
    sql: ${TABLE}.diff_from_previous_load_ts ;;
  }

  dimension: execution_time {
    type: number
    hidden: yes
    sql: ${TABLE}.execution_time ;;
  }

  measure: total_execution_time {
    type: sum
    sql: ${TABLE}.execution_time ;;
  }

  measure: avg_execution_time {
    type: average
    sql: ${TABLE}.execution_time ;;
  }

  dimension_group: load_ts {
    group_label: "Job Execution"
    type: time
    timeframes: [
      raw,
      time
    ]
    sql: ${TABLE}.load_ts ;;
  }

  dimension: object {
    type: string
    sql: ${TABLE}.object ;;
  }

  dimension: pct_diff_from_previous_load_ts {
    type: number
    sql: ${TABLE}.pct_diff_from_previous_load_ts ;;
  }

  measure: avg_pct_diff_from_previous_load_ts {
    type: average
    sql: ${TABLE}.pct_diff_from_previous_load_ts ;;
  }

  dimension: previous_load_ts {
    hidden: yes
    type: number
    sql: ${TABLE}.previous_load_ts ;;
  }

  dimension: row_count {
    type: number
    sql: ${TABLE}.row_count ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }


}
