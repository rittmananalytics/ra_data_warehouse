view: adsets_dim {
  sql_table_name: `ra-development.analytics.adsets_dim`
    ;;

  dimension: account_id {
    type: string
    sql: ${TABLE}.account_id ;;
  }

  dimension: adset_budget_remaining {
    type: number
    sql: ${TABLE}.adset_budget_remaining ;;
  }

  dimension_group: adset_created_ts {
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
    sql: ${TABLE}.adset_created_ts ;;
  }

  dimension: adset_effective_status {
    type: string
    sql: ${TABLE}.adset_effective_status ;;
  }

  dimension_group: adset_end_ts {
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
    sql: ${TABLE}.adset_end_ts ;;
  }

  dimension: adset_id {
    type: string
    sql: ${TABLE}.adset_id ;;
  }

  dimension_group: adset_last_modified_ts {
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
    sql: ${TABLE}.adset_last_modified_ts ;;
  }

  dimension: adset_name {
    type: string
    sql: ${TABLE}.adset_name ;;
  }

  dimension: adset_pk {
    type: string
    sql: ${TABLE}.adset_pk ;;
  }

  dimension_group: adset_start_ts {
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
    sql: ${TABLE}.adset_start_ts ;;
  }

  dimension: adset_targeting {
    hidden: yes
    sql: ${TABLE}.adset_targeting ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}.campaign_id ;;
  }

  measure: count {
    type: count
    drill_fields: [adset_name]
  }
}

view: adsets_dim__adset_targeting {
  dimension: age_max {
    type: number
    sql: ${TABLE}.age_max ;;
  }

  dimension: age_min {
    type: number
    sql: ${TABLE}.age_min ;;
  }

  dimension: device_platforms {
    type: string
    sql: ${TABLE}.device_platforms ;;
  }

  dimension: facebook_positions {
    type: string
    sql: ${TABLE}.facebook_positions ;;
  }

  dimension: geo_locations {
    hidden: yes
    sql: ${TABLE}.geo_locations ;;
  }

  dimension: messenger_positions {
    type: string
    sql: ${TABLE}.messenger_positions ;;
  }

  dimension: publisher_platforms {
    type: string
    sql: ${TABLE}.publisher_platforms ;;
  }
}

view: adsets_dim__adset_targeting__geo_locations__regions {
  dimension: country {
    type: string
    map_layer_name: countries
    sql: ${TABLE}.country ;;
  }

  dimension: key {
    type: string
    sql: ${TABLE}.key ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }
}

view: adsets_dim__adset_targeting__geo_locations__cities {
  dimension: country {
    type: string
    map_layer_name: countries
    sql: ${TABLE}.country ;;
  }

  dimension: distance_unit {
    type: string
    sql: ${TABLE}.distance_unit ;;
  }

  dimension: key {
    type: string
    sql: ${TABLE}.key ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: radius {
    type: number
    sql: ${TABLE}.radius ;;
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
  }

  dimension: region_id {
    type: string
    sql: ${TABLE}.region_id ;;
  }
}

view: adsets_dim__adset_targeting__geo_locations {
  dimension: cities {
    hidden: yes
    sql: ${TABLE}.cities ;;
  }

  dimension: location_types {
    type: string
    sql: ${TABLE}.location_types ;;
  }

  dimension: regions {
    hidden: yes
    sql: ${TABLE}.regions ;;
  }
}
