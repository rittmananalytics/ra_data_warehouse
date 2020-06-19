view: customers {
  sql_table_name: stripe.customers ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
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

  dimension: delinquent {
    type: yesno
    sql: ${TABLE}.delinquent ;;
  }

  dimension: discount_id {
    type: string
    # hidden: true
    sql: ${TABLE}.discount_id ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
    html: <a href="/dashboards/segment_stripe::customer_lookup?Customer%20Email={{ value }}">{{ value }}</a>
      ;;
  }

  dimension_group: received {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.received_at ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      id,
      customers.email,
      customers.created_date,
      discounts.id,
      charges.count,
      discounts.count,
      invoice_items.count,
      invoices.count,
      subscriptions.count
    ]
  }
}
