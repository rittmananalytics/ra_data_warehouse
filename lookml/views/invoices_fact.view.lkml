view: invoices_fact {
  sql_table_name: `mark_bi_apps_dev.invoices_fact`
    ;;

  dimension: all_invoice_ids {
    type: string
    hidden: yes
    sql: ${TABLE}.all_invoice_ids ;;
  }

  dimension: company_id {
    type: string
    hidden: yes

    sql: ${TABLE}.company_id ;;
  }

  dimension: company_pk {
    hidden: yes

    type: string
    sql: ${TABLE}.company_pk ;;
  }

  measure: total_active_clients {
    type: count_distinct
    sql: ${TABLE}.company_pk ;;
  }

  dimension: creator_users_pk {
    hidden: yes

    type: string
    sql: ${TABLE}.creator_users_pk ;;
  }

  dimension_group: invoice_created_at_ts {
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
    sql: ${TABLE}.invoice_created_at_ts ;;
  }

  dimension: invoice_creator_users_id {
    hidden: yes

    type: string
    sql: ${TABLE}.invoice_creator_users_id ;;
  }

  dimension: invoice_currency {
    type: string
    sql: ${TABLE}.invoice_currency ;;
  }

  dimension_group: invoice_due_at_ts {
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
    sql: ${TABLE}.invoice_due_at_ts ;;
  }

  dimension_group: invoice_issue_at_ts {
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
    sql: ${TABLE}.invoice_issue_at_ts ;;
  }

  dimension: invoice_local_total_billed_amount {
    hidden: yes

    type: number
    sql: ${TABLE}.invoice_local_total_billed_amount ;;
  }

  dimension: invoice_local_total_due_amount {
    hidden: yes

    type: number
    sql: ${TABLE}.invoice_local_total_due_amount ;;
  }

  dimension: invoice_local_total_expenses_amount {
    hidden: yes

    type: number
    sql: ${TABLE}.invoice_local_total_expenses_amount ;;
  }



  dimension: invoice_local_total_licence_referral_fee_amount {
    hidden: yes

    type: number
    sql: ${TABLE}.invoice_local_total_licence_referral_fee_amount ;;
  }

  measure: total_local_invoice_licence_referral_fee_amount {
    type: sum
    sql: ${TABLE}.invoice_local_total_licence_referral_fee_amount ;;
  }

  dimension: invoice_local_total_revenue_amount {
    hidden: yes

    type: number
    sql: ${TABLE}.invoice_local_total_revenue_amount ;;
  }

  measure: total_local_invoice_revenue_amount {
    type: sum
    sql: ${TABLE}.total_local_amount ;;
  }

  dimension: invoice_local_total_services_amount {
    hidden: yes

    type: number
    sql: ${TABLE}.invoice_local_total_services_amount ;;
  }

  dimension: invoice_local_total_support_amount {
    hidden: yes

    type: number
    sql: ${TABLE}.invoice_local_total_support_amount ;;
  }

  dimension: invoice_local_total_tax_amount {
    hidden: yes

    type: number
    sql: ${TABLE}.invoice_local_total_tax_amount ;;
  }

  dimension: invoice_number {
    type: string
    sql: ${TABLE}.invoice_number ;;
  }

  dimension_group: invoice_paid_at_ts {
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
    sql: ${TABLE}.invoice_paid_at_ts ;;
  }

  dimension: invoice_payment_term {
    type: string
    sql: ${TABLE}.invoice_payment_term ;;
  }

  dimension_group: invoice_period_end_at_ts {
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
    sql: ${TABLE}.invoice_period_end_at_ts ;;
  }

  dimension_group: invoice_period_start_at_ts {
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
    sql: ${TABLE}.invoice_period_start_at_ts ;;
  }

  dimension: invoice_pk {
    primary_key: yes
    type: string
    sql: ${TABLE}.invoice_pk ;;
  }

  dimension: invoice_seq {
    type: number
    sql: ${TABLE}.invoice_seq ;;
  }

  dimension: months_since_first_invoice {
    type: number
    sql: ${TABLE}.months_since_first_invoice ;;
  }

  dimension_group: first_invoice_month {
    type: time
    timeframes: [
      month
    ]
    sql: ${TABLE}.first_invoice_month ;;
  }



  dimension: quarters_since_first_invoice {
    type: number
    sql: ${TABLE}.quarters_since_first_invoice ;;
  }

  dimension_group: first_invoice_quarter {
    type: time
    timeframes: [
      month
    ]
    sql: ${TABLE}.first_invoice_quarter ;;
  }

  dimension_group: invoice_sent_at_ts {
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
    sql: ${TABLE}.invoice_sent_at_ts ;;
  }

  dimension: invoice_status {
    type: string
    sql: ${TABLE}.invoice_status ;;
  }

  dimension: invoice_total_days_to_pay {
  hidden: yes

  type: number
  sql: ${TABLE}.invoice_total_days_to_pay ;;
  }

  dimension: invoice_total_days_variance_on_payment_terms {
    hidden: yes

    type: number
    sql: ${TABLE}.invoice_total_days_variance_on_payment_terms ;;
  }

  measure: avg_days_variance_on_payment_terms {
    type: average
    sql: ${TABLE}.invoice_total_days_variance_on_payment_terms ;;
  }

  measure: avg_invoice_total_days_overdue {
    type: average
    sql: ${TABLE}.invoice_total_days_overdue ;;
  }

measure: avg_invoice_total_days_to_pay {
type: average
sql: ${TABLE}.invoice_total_days_to_pay ;;
}

dimension: invoice_total_days_overdue {
  hidden: yes

  type: number
  sql: ${TABLE}.invoice_total_days_overdue ;;
}



  dimension: invoice_subject {
    type: string
    sql: ${TABLE}.invoice_subject ;;
  }

  dimension: invoice_tax_rate_pct {
    hidden: yes

    type: string
    sql: ${TABLE}.invoice_tax_rate_pct ;;
  }

  dimension: invoice_type {
    type: string
    sql: ${TABLE}.invoice_type ;;
  }

  dimension: project_id {
    hidden: yes

    type: string
    sql: ${TABLE}.project_id ;;
  }

  dimension: timesheet_project_pk {
    hidden: yes

    type: string
    sql: ${TABLE}.timesheet_project_pk ;;
  }

  dimension: total_local_amount {
    hidden: yes

    type: number
    sql: ${TABLE}.total_local_amount ;;
  }

  measure: total_local_invoice_amount {
    type: sum
    sql: ${TABLE}.total_local_amount ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
