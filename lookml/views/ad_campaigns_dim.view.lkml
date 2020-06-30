view: ad_campaigns_dim {
  sql_table_name: `ad_campaigns_dim`
  label: "Ad Campaigns"
    ;;

  dimension: account_id {
    type: string
    sql: ${TABLE}.account_id ;;
  }

  dimension: campaign_buying_type {
    type: string
    sql: ${TABLE}.campaign_buying_type ;;
  }

  dimension: campaign_effective_status {
    type: string
    sql: ${TABLE}.campaign_effective_status ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}.campaign_id ;;
  }

  dimension_group: campaign_last_modified_ts {
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
    sql: ${TABLE}.campaign_last_modified_ts ;;
  }

  dimension: campaign_name {
    type: string
    sql: ${TABLE}.campaign_name ;;
  }

  dimension: campaign_objective {
    type: string
    sql: ${TABLE}.campaign_objective ;;
  }

  dimension: campaign_pk {
    type: string
    sql: ${TABLE}.campaign_pk ;;
  }

  dimension_group: campaign_start_ts {
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
    sql: ${TABLE}.campaign_start_ts ;;
  }

  measure: count {
    type: count
    drill_fields: [campaign_name]
  }
}
