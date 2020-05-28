view: email_lists_dim {
  sql_table_name: `email_lists_dim`
    ;;

  dimension: audience_name {
    type: string
    sql: ${TABLE}.audience_name ;;
  }

  dimension: avg_sub_rate_pct {
    type: number
    sql: ${TABLE}.avg_sub_rate_pct ;;
  }

  dimension: avg_unsub_rate_pct {
    type: number
    sql: ${TABLE}.avg_unsub_rate_pct ;;
  }

  dimension_group: campaign_last_sent_ts {
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
    sql: ${TABLE}.campaign_last_sent_ts ;;
  }

  dimension: click_rate_pct {
    type: number
    sql: ${TABLE}.click_rate_pct ;;
  }

  dimension_group: last_sub_ts {
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
    sql: ${TABLE}.last_sub_ts ;;
  }

  dimension_group: last_unsub_ts {
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
    sql: ${TABLE}.last_unsub_ts ;;
  }

  dimension: list_id {
    type: string
    sql: ${TABLE}.list_id ;;
  }

  dimension: list_pk {
    primary_key: yes

    type: string
    sql: ${TABLE}.list_pk ;;
  }

  dimension: open_rate_pct {
    type: number
    sql: ${TABLE}.open_rate_pct ;;
  }

  dimension: target_sub_rate_pct {
    type: number
    sql: ${TABLE}.target_sub_rate_pct ;;
  }

  dimension: total_campaigns {
    type: number
    sql: ${TABLE}.total_campaigns ;;
  }

  dimension: total_cleaned {
    type: number
    sql: ${TABLE}.total_cleaned ;;
  }

  dimension: total_cleaned_since_send {
    type: number
    sql: ${TABLE}.total_cleaned_since_send ;;
  }

  dimension: total_members {
    type: number
    sql: ${TABLE}.total_members ;;
  }

  dimension: total_members_since_send {
    type: number
    sql: ${TABLE}.total_members_since_send ;;
  }

  dimension: total_merge_fields {
    type: number
    sql: ${TABLE}.total_merge_fields ;;
  }

  dimension: total_unsubscribes {
    type: number
    sql: ${TABLE}.total_unsubscribes ;;
  }

  dimension: total_unsubscribes_since_send {
    type: number
    sql: ${TABLE}.total_unsubscribes_since_send ;;
  }

  measure: count {
    type: count
    drill_fields: [audience_name]
  }
}