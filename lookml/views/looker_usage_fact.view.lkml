view: looker_usage_fact {
  sql_table_name: `looker_usage_fact`
    ;;

  dimension: company_pk {
    type: string
    sql: ${TABLE}.company_pk ;;
  }

  dimension: usage_database_type {
    type: string
    sql: ${TABLE}.usage_database_type ;;
  }

  dimension: usage_feature_title {
    type: string
    sql: ${TABLE}.usage_feature_title ;;
  }

  dimension: usage_id {
    type: string
    sql: ${TABLE}.usage_id ;;
  }

  dimension: usage_originator_type {
    type: string
    sql: ${TABLE}.usage_originator_type ;;
  }

  dimension: usage_pk {
    type: string
    sql: ${TABLE}.usage_pk ;;
  }

  dimension: usage_response_time_secs {
    type: number
    sql: ${TABLE}.usage_response_time_secs ;;
  }

  dimension: usage_status {
    type: string
    sql: ${TABLE}.usage_status ;;
  }

  dimension: usage_subject_area {
    type: string
    sql: ${TABLE}.usage_subject_area ;;
  }

  dimension: usage_time_elapsed_mins {
    type: number
    sql: ${TABLE}.usage_time_elapsed_mins ;;
  }

  dimension: usage_triggered_cache_reload {
    type: string
    sql: ${TABLE}.usage_triggered_cache_reload ;;
  }

  dimension_group: usage_ts {
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
    sql: CAST(${TABLE}.usage_ts AS TIMESTAMP) ;;
  }

  dimension: user_name {
    type: string
    sql: ${TABLE}.user_name ;;
  }

  measure: count {
    type: count
    drill_fields: [user_name]
  }
}