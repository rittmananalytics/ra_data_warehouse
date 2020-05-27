view: email_send_outcomes_fact {
  sql_table_name: `mark_bi_apps_dev.email_send_outcomes_fact`
    ;;

  dimension: action {
    type: string
    sql: ${TABLE}.action ;;
  }

  dimension: contact_pk {
    type: string
    sql: ${TABLE}.contact_pk ;;
  }

  dimension: email_address {
    type: string
    sql: ${TABLE}.email_address ;;
  }

  dimension: list_pk {
    type: string
    sql: ${TABLE}.list_pk ;;
  }

  dimension: send_outcome_pk {
    type: string
    sql: ${TABLE}.send_outcome_pk ;;
  }

  dimension: send_pk {
    type: string
    sql: ${TABLE}.send_pk ;;
  }

  dimension_group: timestamp {
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
    sql: ${TABLE}.timestamp ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
