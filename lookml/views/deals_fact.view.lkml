view: deals_fact {
  sql_table_name: `mark_bi_apps_dev.deals_fact`
    ;;

  dimension: company_pk {
    type: string
    sql: ${TABLE}.company_pk ;;
  }

  dimension: deal_amount {
    type: number
    sql: ${TABLE}.deal_amount ;;
  }

  dimension: deal_assigned_consultant_users_pk {
    type: string
    sql: ${TABLE}.deal_assigned_consultant_users_pk ;;
  }

  dimension_group: deal_closed {
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
    sql: ${TABLE}.deal_closed_date ;;
  }

  dimension: deal_closed_lost_reason {
    type: string
    sql: ${TABLE}.deal_closed_lost_reason ;;
  }

  dimension: deal_components {
    type: string
    sql: ${TABLE}.deal_components ;;
  }

  dimension: deal_count_components {
    type: string
    sql: ${TABLE}.deal_count_components ;;
  }

  dimension_group: deal_created_ts {
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
    sql: ${TABLE}.deal_created_ts ;;
  }

  dimension: deal_days_to_close {
    type: number
    sql: ${TABLE}.deal_days_to_close ;;
  }

  dimension: deal_days_until_end {
    type: number
    sql: ${TABLE}.deal_days_until_end ;;
  }

  dimension_group: deal_delivery_end_date_ts {
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
    sql: ${TABLE}.deal_delivery_end_date_ts ;;
  }

  dimension_group: deal_delivery_schedule_ts {
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
    sql: ${TABLE}.deal_delivery_schedule_ts ;;
  }

  dimension_group: deal_delivery_start_ts {
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
    sql: ${TABLE}.deal_delivery_start_ts ;;
  }

  dimension: deal_description {
    type: string
    sql: ${TABLE}.deal_description ;;
  }

  dimension: deal_duration_days {
    type: number
    sql: ${TABLE}.deal_duration_days ;;
  }

  dimension: deal_id {
    type: number
    sql: ${TABLE}.deal_id ;;
  }

  dimension: deal_is_closed {
    type: yesno
    sql: ${TABLE}.deal_is_closed ;;
  }

  dimension_group: deal_last_modified_ts {
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
    sql: ${TABLE}.deal_last_modified_ts ;;
  }

  dimension: deal_local_amount {
    type: number
    sql: ${TABLE}.deal_local_amount ;;
  }

  dimension: deal_name {
    type: string
    sql: ${TABLE}.deal_name ;;
  }

  dimension: deal_number_of_sprints {
    type: number
    sql: ${TABLE}.deal_number_of_sprints ;;
  }

  dimension: deal_owner_id {
    type: string
    sql: ${TABLE}.deal_owner_id ;;
  }

  dimension: deal_partner_referral_type {
    type: string
    sql: ${TABLE}.deal_partner_referral_type ;;
  }

  dimension: deal_pipeline_name {
    type: string
    sql: ${TABLE}.deal_pipeline_name ;;
  }

  dimension: deal_pk {
    type: string
    sql: ${TABLE}.deal_pk ;;
  }

  dimension: deal_pricing_model {
    type: string
    sql: ${TABLE}.deal_pricing_model ;;
  }

  dimension: deal_probability_pct {
    type: number
    sql: ${TABLE}.deal_probability_pct ;;
  }

  dimension: deal_products_in_solution {
    type: string
    sql: ${TABLE}.deal_products_in_solution ;;
  }

  dimension: deal_salesperson_users_pk {
    type: string
    sql: ${TABLE}.deal_salesperson_users_pk ;;
  }

  dimension: deal_source {
    type: string
    sql: ${TABLE}.deal_source ;;
  }

  dimension: deal_sprint_type {
    type: string
    sql: ${TABLE}.deal_sprint_type ;;
  }

  dimension: deal_stage_display_order {
    type: number
    sql: ${TABLE}.deal_stage_display_order ;;
  }

  dimension: deal_stage_id {
    type: string
    sql: ${TABLE}.deal_stage_id ;;
  }

  dimension: deal_stage_label {
    type: string
    sql: ${TABLE}.deal_stage_label ;;
  }

  dimension: deal_stage_name {
    type: string
    sql: ${TABLE}.deal_stage_name ;;
  }

  dimension_group: deal_stage_ts {
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
    sql: ${TABLE}.deal_stage_ts ;;
  }

  dimension: deal_type {
    type: string
    sql: ${TABLE}.deal_type ;;
  }

  dimension: is_active {
    type: yesno
    sql: ${TABLE}.is_active ;;
  }

  dimension: pipeline_label {
    type: string
    sql: ${TABLE}.pipeline_label ;;
  }

  measure: count {
    type: count
    drill_fields: [deal_name, deal_stage_name, deal_pipeline_name]
  }
}
