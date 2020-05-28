view: transactions_fact {
  sql_table_name: `transactions_fact`
    ;;

  dimension_group: transaction_created_ts {
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
    sql: ${TABLE}.transaction_created_ts ;;
  }

  dimension: transaction_currency {
    type: string
    sql: ${TABLE}.transaction_currency ;;
  }

  dimension: transaction_description {
    type: string
    sql: ${TABLE}.transaction_description ;;
  }

  dimension: transaction_exchange_rate {
    type: number
    sql: ${TABLE}.transaction_exchange_rate ;;
  }

  dimension: transaction_fee_amount {
    type: number
    sql: ${TABLE}.transaction_fee_amount ;;
  }

  dimension: transaction_gross_amount {
    type: number
    sql: ${TABLE}.transaction_gross_amount ;;
  }

  dimension: transaction_id {
    type: string
    sql: ${TABLE}.transaction_id ;;
  }

  dimension: transaction_net_amount {
    type: number
    sql: ${TABLE}.transaction_net_amount ;;
  }

  dimension: transaction_pk {
    type: string
    sql: ${TABLE}.transaction_pk ;;
  }

  dimension: transaction_status {
    type: string
    sql: ${TABLE}.transaction_status ;;
  }

  dimension: transaction_tax_amount {
    type: number
    sql: ${TABLE}.transaction_tax_amount ;;
  }

  dimension: transaction_type {
    type: string
    sql: ${TABLE}.transaction_type ;;
  }

  dimension_group: transaction_updated_ts {
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
    sql: ${TABLE}.transaction_updated_ts ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}