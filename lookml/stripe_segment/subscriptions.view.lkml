view: subscriptions {
  sql_table_name: stripe.subscriptions ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: cancel_at_period_end {
    type: yesno
    sql: ${TABLE}.cancel_at_period_end ;;
  }

  dimension_group: canceled {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.canceled_at ;;
  }

  dimension_group: current_period_end {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.current_period_end ;;
  }

  dimension_group: current_period_start {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.current_period_start ;;
  }

  dimension: customer_id {
    type: string
    # hidden: true
    sql: ${TABLE}.customer_id ;;
  }

  dimension: discount_id {
    type: string
    # hidden: true
    sql: ${TABLE}.discount_id ;;
  }

  dimension: plan_id {
    type: string
    # hidden: true
    sql: ${TABLE}.plan_id ;;
  }

  dimension: quantity {
    type: number
    sql: ${TABLE}.quantity ;;
  }

  dimension_group: received {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.received_at ;;
  }

  dimension_group: start {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.start ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension_group: trial_end {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.trial_end ;;
  }

  dimension_group: trial_start {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.trial_start ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      id,
      customers.id,
      discounts.id,
      plans.id,
      plans.name,
      invoice_items.count,
      invoice_lines.count,
      invoices.count
    ]
  }
}
