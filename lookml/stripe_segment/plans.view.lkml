view: plans {
  sql_table_name: stripe.plans ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }

  dimension_group: created {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.created ;;
  }

  dimension: currency {
    type: string
    sql: ${TABLE}.currency ;;
  }

  dimension: interval {
    type: string
    sql: ${TABLE}.interval ;;
  }

  dimension: interval_count {
    type: number
    sql: ${TABLE}.interval_count ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension_group: received {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.received_at ;;
  }

  dimension: trial_period_days {
    type: number
    sql: ${TABLE}.trial_period_days ;;
  }

  measure: count {
    type: count
    drill_fields: [id, name, invoice_items.count, subscriptions.count]
  }
}
