view: invoices {
  sql_table_name: stripe.invoices ;;
  ## Dimensions

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
    html: <a href="/dashboards/segment_stripe::invoice_lookup?invoice_id={{ value }}">{{ value }}</a>
      ;;
  }

  dimension: amount_due {
    type: number
    sql: ${TABLE}.amount_due/100.0 ;;
    value_format: "[>=1000000]$0.00,,\"M\";[>=1000]$0.00,\"K\";$0.00"
  }

  dimension: attempt_count {
    type: number
    sql: ${TABLE}.attempt_count ;;
  }

  dimension: attempted {
    type: yesno
    sql: ${TABLE}.attempted ;;
  }

  dimension: charge_id {
    type: string
    # hidden: true
    sql: ${TABLE}.charge_id ;;
  }

  dimension: closed {
    type: yesno
    sql: ${TABLE}.closed ;;
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

  dimension: ending_balance {
    type: number
    sql: ${TABLE}.ending_balance ;;
  }

  dimension_group: next_payment_attempt {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.next_payment_attempt ;;
  }

  dimension: paid {
    type: yesno
    sql: ${TABLE}.paid ;;
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

  dimension_group: received {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.received_at ;;
  }

  dimension: starting_balance {
    type: number
    sql: ${TABLE}.starting_balance ;;
  }

  dimension: subscription_id {
    type: string
    # hidden: true
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: subtotal {
    type: number
    sql: ${TABLE}.subtotal*1.0/100 ;;
    value_format: "$#,##0.00"
  }

  dimension: total {
    type: number
    sql: ${TABLE}.total*1.0/100 ;;
    value_format: "$#,##0.00"
  }

  dimension_group: webhooks_delivered {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.webhooks_delivered_at ;;
  }

  ## Measures

  measure: total_amount_due {
    type: sum
    sql: ${amount_due} ;;
    value_format: "$#,##0.00"
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      id,
      charges.id,
      subscriptions.id,
      charges.created_date,
      customers.id,
      customers.email,
      invoice_items.count
    ]
  }
}
