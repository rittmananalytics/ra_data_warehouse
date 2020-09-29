view: ad_campaign_performance_fact {
  sql_table_name: `ra-development.analytics.ad_campaign_performance_fact`
    ;;

  dimension: actual_cpc {
    type: number
    sql: ${TABLE}.actual_cpc ;;
  }

  dimension: actual_ctr {
    type: number
    sql: ${TABLE}.actual_ctr ;;
  }

  dimension: actual_vs_reported_clicks_pct {
    type: number
    sql: ${TABLE}.actual_vs_reported_clicks_pct ;;
  }

  dimension: avg_reported_bounce_rate {
    type: number
    sql: ${TABLE}.avg_reported_bounce_rate ;;
  }

  dimension: avg_reported_time_on_site {
    type: number
    sql: ${TABLE}.avg_reported_time_on_site ;;
  }

  dimension_group: campaign {
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
    sql: ${TABLE}.campaign_date ;;
  }

  dimension: reported_cpc {
    type: number
    sql: ${TABLE}.reported_cpc ;;
  }

  dimension: reported_cpm {
    type: number
    sql: ${TABLE}.reported_cpm ;;
  }

  dimension: reported_ctr {
    type: number
    sql: ${TABLE}.reported_ctr ;;
  }

  dimension: total_clicks {
    type: number
    sql: ${TABLE}.total_clicks ;;
  }

  dimension: total_reported_clicks {
    type: number
    sql: ${TABLE}.total_reported_clicks ;;
  }

  dimension: total_reported_cost {
    type: number
    sql: ${TABLE}.total_reported_cost ;;
  }

  dimension: total_reported_impressions {
    type: number
    sql: ${TABLE}.total_reported_impressions ;;
  }

  dimension: total_reported_invalid_clicks {
    type: number
    sql: ${TABLE}.total_reported_invalid_clicks ;;
  }

  dimension: utm_campaign {
    type: string
    sql: ${TABLE}.utm_campaign ;;
  }

  dimension: utm_source {
    type: string
    sql: ${TABLE}.utm_source ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
