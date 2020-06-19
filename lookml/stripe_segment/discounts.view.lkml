view: discounts {
  sql_table_name: stripe.discounts ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension_group: _end {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}._end ;;
  }

  dimension: customer_id {
    type: string
    # hidden: true
    sql: ${TABLE}.customer_id ;;
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

  dimension: subscription {
    type: string
    sql: ${TABLE}.subscription ;;
  }

  measure: count {
    type: count
    drill_fields: [id, customers.id, customers.count, subscriptions.count]
  }
}
