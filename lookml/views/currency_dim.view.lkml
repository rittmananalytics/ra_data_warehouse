view: currency_dim {
  sql_table_name: `mark_bi_apps_dev.currency_dim`
    ;;

  dimension: currency_code {
    type: string
    sql: ${TABLE}.currency_code ;;
  }

  dimension: currency_name {
    type: string
    sql: ${TABLE}.currency_name ;;
  }

  dimension: currency_pk {
    type: string
    sql: ${TABLE}.currency_pk ;;
  }

  measure: count {
    type: count
    drill_fields: [currency_name]
  }
}
