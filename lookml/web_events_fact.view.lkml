view: web_events_fact {
  sql_table_name: `ra-development.analytics.web_events_fact`
    ;;

  dimension: blended_user_id {
    type: string
    hidden: yes
    sql: ${TABLE}.blended_user_id ;;
  }

  dimension: converting_page_title {
    group_label: "Conversions"
    label: "Converting Page Title"
    description: ""
    type: string
    sql: ${TABLE}.converting_page_title ;;
  }

  dimension: converting_page_url {
    group_label: "Conversions"
    label: "Converting Page URL"
    description: ""
    type: string
    sql: ${TABLE}.converting_page_url ;;
  }

  dimension: session_id {
    type: string
    hidden: yes
    sql: ${TABLE}.session_id ;;
  }


   dimension: event_details {
    group_label: "Behavior"
    description: ""
    type: string
    sql: ${TABLE}.event_details ;;
  }

  dimension: event_id {
    hidden: yes
    type: string
    sql: ${TABLE}.event_id ;;
  }

  dimension: event_num {
    hidden: yes

    type: number
    sql: ${TABLE}.event_num ;;
  }

  dimension_group: event_ts {
    group_label: "Dates"
    label: "Event"
    description: ""
    type: time
    timeframes: [
      date
    ]
    sql: ${TABLE}.event_ts ;;
  }

  dimension: event_type {
    group_label: "Behavior"
    description: ""
    type: string
    sql: ${TABLE}.event_type ;;
  }

  dimension: gclid {
    type: string
    hidden: yes
    sql: ${TABLE}.gclid ;;
  }

  dimension: ip {
    hidden: yes
    type: string
    sql: ${TABLE}.ip ;;
  }

  dimension: page_title {
    group_label: "Behavior"

    type: string
    sql: ${TABLE}.page_title ;;
  }

  dimension: page_url {
    group_label: "Behavior"

    type: string
    sql: ${TABLE}.page_url ;;
  }

  dimension: page_url_host {
    group_label: "Behavior"

    type: string
    sql: ${TABLE}.page_url_host ;;
  }

  dimension: page_url_path {
    group_label: "Behavior"

    type: string
    sql: ${TABLE}.page_url_path ;;
  }

  dimension: page_view_count {
    hidden: yes
    type: number
    sql: ${TABLE}.page_view_count ;;
  }

  dimension: pre_converting_page_title {
    group_label: "Conversions"
    type: string
    sql: ${TABLE}.pre_converting_page_title ;;
  }

  dimension: pre_converting_page_url {
    group_label: "Conversions"

    type: string
    sql: ${TABLE}.pre_converting_page_url ;;
  }

  dimension_group: prev_event_ts {
    type: time
    hidden: yes
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.prev_event_ts ;;
  }

  dimension: prev_event_type {
    group_label: "Conversions"
    hidden: yes
    type: string
    sql: ${TABLE}.prev_event_type ;;
  }

  dimension: session_event_num {
    group_label: "Behavior"

    type: number
    sql: ${TABLE}.session_event_num ;;
  }


  dimension: site {
    type: string
    sql: ${TABLE}.site ;;
  }

  dimension: time_on_page_secs {
    hidden: yes

    type: number
    sql: ${TABLE}.time_on_page_secs ;;
  }

  dimension: web_event_pk {
    hidden: yes
    primary_key: yes
    type: string
    sql: ${TABLE}.web_event_pk ;;
  }

  measure: total_page_views {
    hidden: no
    type: count_distinct
    sql: ${TABLE}.web_event_pk ;;
  }
}
