view: ad_performance_snapshot_fact {
  sql_table_name: `mark_bi_apps_dev.ad_performance_snapshot_fact`
    ;;

  dimension: account_id {
    type: string
    sql: ${TABLE}.account_id ;;
  }

  dimension: ad_action_type {
    type: string
    sql: ${TABLE}.ad_action_type ;;
  }

  dimension: ad_action_value {
    type: number
    sql: ${TABLE}.ad_action_value ;;
  }

  dimension: ad_performance_snapshot_pk {
    type: string
    sql: ${TABLE}.ad_performance_snapshot_pk ;;
  }

  dimension: ad_pk {
    type: string
    sql: ${TABLE}.ad_pk ;;
  }

  dimension_group: ad_snapshot_ts {
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
    sql: ${TABLE}.ad_snapshot_ts ;;
  }

  dimension: ad_total_1d_clicks {
    type: number
    sql: ${TABLE}.ad_total_1d_clicks ;;
  }

  dimension: ad_total_28d_clicks {
    type: number
    sql: ${TABLE}.ad_total_28d_clicks ;;
  }

  dimension: ad_total_7d_clicks {
    type: number
    sql: ${TABLE}.ad_total_7d_clicks ;;
  }

  dimension: adset_pk {
    type: string
    sql: ${TABLE}.adset_pk ;;
  }

  dimension: campaign_pk {
    type: string
    sql: ${TABLE}.campaign_pk ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
