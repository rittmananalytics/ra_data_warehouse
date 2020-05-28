view: chart_of_accounts_dim {
  sql_table_name: `chart_of_accounts_dim`
    ;;

  dimension: account_bank_account_number {
    type: string
    sql: ${TABLE}.account_bank_account_number ;;
  }

  dimension: account_bank_account_type {
    type: string
    sql: ${TABLE}.account_bank_account_type ;;
  }

  dimension: account_class {
    type: string
    sql: ${TABLE}.account_class ;;
  }

  dimension: account_code {
    type: string
    sql: ${TABLE}.account_code ;;
  }

  dimension: account_currency_code {
    type: string
    sql: ${TABLE}.account_currency_code ;;
  }

  dimension: account_description {
    type: string
    sql: ${TABLE}.account_description ;;
  }

  dimension: account_enable_payments_to_account {
    type: yesno
    sql: ${TABLE}.account_enable_payments_to_account ;;
  }

  dimension: account_id {
    type: string
    sql: ${TABLE}.account_id ;;
  }

  dimension: account_is_system_account {
    type: string
    sql: ${TABLE}.account_is_system_account ;;
  }

  dimension: account_name {
    type: string
    sql: ${TABLE}.account_name ;;
  }

  dimension: account_pk {
    type: string
    sql: ${TABLE}.account_pk ;;
  }

  dimension: account_reporting_code {
    type: string
    sql: ${TABLE}.account_reporting_code ;;
  }

  dimension: account_reporting_code_name {
    type: string
    sql: ${TABLE}.account_reporting_code_name ;;
  }

  dimension: account_show_in_expense_claims {
    type: yesno
    sql: ${TABLE}.account_show_in_expense_claims ;;
  }

  dimension: account_status {
    type: string
    sql: ${TABLE}.account_status ;;
  }

  dimension: account_tax_type {
    type: string
    sql: ${TABLE}.account_tax_type ;;
  }

  dimension: account_type {
    type: string
    sql: ${TABLE}.account_type ;;
  }

  measure: count {
    type: count
    drill_fields: [account_reporting_code_name, account_name]
  }
}