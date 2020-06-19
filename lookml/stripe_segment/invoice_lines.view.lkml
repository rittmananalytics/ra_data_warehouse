view: invoice_lines {
  sql_table_name: stripe.invoice_lines ;;

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

  dimension: discountable {
    type: yesno
    sql: ${TABLE}.discountable ;;
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

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  measure: count {
    type: count
    drill_fields: [id, subscriptions.id]
  }
}
