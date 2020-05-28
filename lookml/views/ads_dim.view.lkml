view: ads_dim {
  sql_table_name: `ads_dim`
    ;;

  dimension: account_id {
    type: string
    sql: ${TABLE}.account_id ;;
  }

  dimension: ad_bid_type {
    type: string
    sql: ${TABLE}.ad_bid_type ;;
  }

  dimension: ad_conversion_specs {
    hidden: yes
    sql: ${TABLE}.ad_conversion_specs ;;
  }

  dimension_group: ad_created_ts {
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
    sql: ${TABLE}.ad_created_ts ;;
  }

  dimension: ad_creative {
    hidden: yes
    sql: ${TABLE}.ad_creative ;;
  }

  dimension: ad_effective_status {
    type: string
    sql: ${TABLE}.ad_effective_status ;;
  }

  dimension: ad_id {
    type: string
    sql: ${TABLE}.ad_id ;;
  }

  dimension_group: ad_last_modified_ts {
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
    sql: ${TABLE}.ad_last_modified_ts ;;
  }

  dimension: ad_last_updated_by_app_id {
    type: string
    sql: ${TABLE}.ad_last_updated_by_app_id ;;
  }

  dimension: ad_name {
    type: string
    sql: ${TABLE}.ad_name ;;
  }

  dimension: ad_pk {
    type: string
    sql: ${TABLE}.ad_pk ;;
  }

  dimension: ad_recommendations {
    hidden: yes
    sql: ${TABLE}.ad_recommendations ;;
  }

  dimension: ad_status {
    type: string
    sql: ${TABLE}.ad_status ;;
  }

  dimension: ad_targeting {
    hidden: yes
    sql: ${TABLE}.ad_targeting ;;
  }

  dimension: ad_tracking_specs {
    hidden: yes
    sql: ${TABLE}.ad_tracking_specs ;;
  }

  dimension: adset_id {
    type: string
    sql: ${TABLE}.adset_id ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}.campaign_id ;;
  }

  dimension: source_ad_id {
    type: string
    sql: ${TABLE}.source_ad_id ;;
  }

  measure: count {
    type: count
    drill_fields: [ad_name]
  }
}

view: ads_dim__ad_recommendations {
  dimension: blame_field {
    type: string
    sql: ${TABLE}.blame_field ;;
  }

  dimension: code {
    type: number
    sql: ${TABLE}.code ;;
  }

  dimension: confidence {
    type: string
    sql: ${TABLE}.confidence ;;
  }

  dimension: importance {
    type: string
    sql: ${TABLE}.importance ;;
  }

  dimension: message {
    type: string
    sql: ${TABLE}.message ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }
}

view: ads_dim__ad_tracking_specs {
  dimension: action_type {
    type: string
    sql: ${TABLE}.action_type ;;
  }

  dimension: fb_pixel {
    type: string
    sql: ${TABLE}.fb_pixel ;;
  }

  dimension: page {
    type: string
    sql: ${TABLE}.page ;;
  }

  dimension: post {
    type: string
    sql: ${TABLE}.post ;;
  }

  dimension: post_wall {
    type: string
    sql: ${TABLE}.post_wall ;;
  }
}

view: ads_dim__ad_conversion_specs {
  dimension: action_type {
    type: string
    sql: ${TABLE}.action_type ;;
  }
}

view: ads_dim__ad_creative {
  drill_fields: [id]

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }
}

view: ads_dim__ad_targeting {
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

view: ads_dim__ad_targeting__geo_locations__regions {
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

view: ads_dim__ad_targeting__geo_locations__cities {
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

view: ads_dim__ad_targeting__geo_locations {
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