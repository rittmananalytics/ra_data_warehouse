view: web_pageviews_fact {
  sql_table_name: `mark_bi_apps_dev.web_pageviews_fact`
    ;;

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: device {
    type: string
    sql: ${TABLE}.device ;;
  }

  dimension: device_category {
    type: string
    sql: ${TABLE}.device_category ;;
  }

  dimension: gclid {
    type: string
    sql: ${TABLE}.gclid ;;
  }

  dimension: ip {
    type: string
    sql: ${TABLE}.ip ;;
  }

  dimension: page_title {
    type: string
    sql: ${TABLE}.page_title ;;
  }

  dimension: page_url {
    type: string
    sql: ${TABLE}.page_url ;;
  }

  dimension: page_url_host {
    type: string
    sql: ${TABLE}.page_url_host ;;
  }

  dimension: page_url_path {
    type: string
    sql: ${TABLE}.page_url_path ;;
  }

  dimension: page_url_query {
    type: string
    sql: ${TABLE}.page_url_query ;;
  }

  dimension: page_view_id {
    type: string
    sql: ${TABLE}.page_view_id ;;
  }

  dimension: page_view_number {
    type: number
    sql: ${TABLE}.page_view_number ;;
  }

  dimension_group: received_at_tstamp {
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
    sql: ${TABLE}.received_at_tstamp ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: referrer_host {
    type: string
    sql: ${TABLE}.referrer_host ;;
  }

  dimension_group: sent_at_tstamp {
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
    sql: ${TABLE}.sent_at_tstamp ;;
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension_group: tstamp {
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
    sql: ${TABLE}.tstamp ;;
  }

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
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

  dimension: web_pageview_pk {
    type: string
    sql: ${TABLE}.web_pageview_pk ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
