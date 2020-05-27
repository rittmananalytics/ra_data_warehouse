view: web_sessions_fact {
  sql_table_name: `mark_bi_apps_dev.web_sessions_fact`
    ;;

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: blended_user_id {
    type: string
    sql: ${TABLE}.blended_user_id ;;
  }

  dimension: device {
    type: string
    sql: ${TABLE}.device ;;
  }

  dimension: device_category {
    type: string
    sql: ${TABLE}.device_category ;;
  }

  dimension: duration_in_s {
    type: number
    sql: ${TABLE}.duration_in_s ;;
  }

  dimension: duration_in_s_tier {
    type: string
    sql: ${TABLE}.duration_in_s_tier ;;
  }

  dimension: first_page_url {
    type: string
    sql: ${TABLE}.first_page_url ;;
  }

  dimension: first_page_url_host {
    type: string
    sql: ${TABLE}.first_page_url_host ;;
  }

  dimension: first_page_url_path {
    type: string
    sql: ${TABLE}.first_page_url_path ;;
  }

  dimension: first_page_url_query {
    type: string
    sql: ${TABLE}.first_page_url_query ;;
  }

  dimension: gclid {
    type: string
    sql: ${TABLE}.gclid ;;
  }

  dimension: last_page_url {
    type: string
    sql: ${TABLE}.last_page_url ;;
  }

  dimension: last_page_url_host {
    type: string
    sql: ${TABLE}.last_page_url_host ;;
  }

  dimension: last_page_url_path {
    type: string
    sql: ${TABLE}.last_page_url_path ;;
  }

  dimension: last_page_url_query {
    type: string
    sql: ${TABLE}.last_page_url_query ;;
  }

  dimension: mins_between_sessions {
    type: number
    sql: ${TABLE}.mins_between_sessions ;;
  }

  dimension: page_views {
    type: number
    sql: ${TABLE}.page_views ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: referrer_host {
    type: string
    sql: ${TABLE}.referrer_host ;;
  }

  dimension: referrer_medium {
    type: string
    sql: ${TABLE}.referrer_medium ;;
  }

  dimension: referrer_source {
    type: string
    sql: ${TABLE}.referrer_source ;;
  }

  dimension_group: session_end_tstamp {
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
    sql: ${TABLE}.session_end_tstamp ;;
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension_group: session_start_tstamp {
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
    sql: ${TABLE}.session_start_tstamp ;;
  }

  dimension: utm_campaign {
    type: string
    sql: ${TABLE}.utm_campaign ;;
  }

  dimension: utm_content {
    type: string
    sql: ${TABLE}.utm_content ;;
  }

  dimension: utm_medium {
    type: string
    sql: ${TABLE}.utm_medium ;;
  }

  dimension: utm_source {
    type: string
    sql: ${TABLE}.utm_source ;;
  }

  dimension: utm_term {
    type: string
    sql: ${TABLE}.utm_term ;;
  }

  dimension: web_session_pk {
    type: string
    sql: ${TABLE}.web_session_pk ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
