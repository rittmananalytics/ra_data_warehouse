view: invoice_items {
  sql_table_name: stripe.invoice_items ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }

  dimension: currency {
    type: string
    sql: ${TABLE}.currency ;;
  }

  dimension: customer_id {
    type: string
    # hidden: true
    sql: ${TABLE}.customer_id ;;
  }

  dimension_group: date {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.date ;;
  }

  dimension: invoice_id {
    type: string
    # hidden: true
    sql: ${TABLE}.invoice_id ;;
  }

  dimension_group: period_end {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.period_end ;;
  }

  dimension_group: period_start {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.period_start ;;
  }

  dimension: plan_id {
    type: string
    # hidden: true
    sql: ${TABLE}.plan_id ;;
  }

  dimension: proration {
    type: yesno
    sql: ${TABLE}.proration ;;
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

  dimension: subscription_id {
    type: string
    # hidden: true
    sql: ${TABLE}.subscription_id ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  ### Custom Fields:

  measure: total_amount {
    type: sum
    sql: ${TABLE}.amount ;;
    value_format: "$#,##0.00"
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      id,
      customers.id,
      invoices.id,
      plans.id,
      plans.name,
      subscriptions.id
    ]
  }
}
