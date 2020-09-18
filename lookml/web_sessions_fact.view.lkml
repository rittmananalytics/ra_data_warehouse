view: web_sessions_fact {
  sql_table_name: `ra-development.analytics.web_sessions_fact`
    ;;

  dimension: blended_user_id {
    type: string
    hidden: yes
    sql: ${TABLE}.blended_user_id ;;
  }

  measure: total_blended_user_id {
    label: "Total Unique Users"
    description: "The total number of unique people viewing the site."
    type: count_distinct
    sql: ${TABLE}.blended_user_id ;;
    drill_fields: [device, blended_user_id, device_category, channel]
  }

  dimension: channel {
    group_label: "    Acquisition"
    label: "Marketing Channel"
    description: "The types of channel through which users can visit the site. E.g Direct, Organic, Paid Search etc."
    type: string
    sql: ${TABLE}.channel ;;
  }


  dimension: device {
    group_label: "  Audience"
    description: "The type of device used to access the page, e.g Android, Macintosh, windows etc. This usually includes the device model and operating system version."
    type: string
    sql: ${TABLE}.device ;;
  }

  dimension: device_category {
    group_label: "  Audience"
    description: "A simplified version of Device field without OS/Browser detail. e.g 'iPhone','Android','iPad','Windows' etc"
    type: string
    sql: ${TABLE}.device_category ;;
  }

  dimension: duration_in_s {
    hidden: yes
    type: number
    sql: ${TABLE}.duration_in_s ;;
  }

  measure: total_duration_in_s {
    description: "The time spanned from the beginning to the end of a session in Seconds."
    type: average
    label: "Avg Session Duration (Secs)"
    sql: ${TABLE}.duration_in_s ;;
  }

  dimension: duration_in_s_tier {
    group_label: "Behavior"
    label: "Session Duration Tier (Secs)"
    description: "The discrete time bracket the session duration falls into. Available Brackets are: '0s to 9s', '10s to 29s','30s to 59s' and '60s or more'."
    type: string
    sql: ${TABLE}.duration_in_s_tier ;;
  }

  dimension: events {
    type: number
    hidden: yes
    sql: ${TABLE}.events ;;
  }

  dimension: first_page_url {
    hidden: yes

    type: string
    sql: ${TABLE}.first_page_url ;;
  }

  dimension: first_page_url_host {
    hidden: yes

    type: string
    sql: ${TABLE}.first_page_url_host ;;
  }

  dimension: first_page_url_path {
    group_label: "Behavior"
    label: "Entrance Page Path"
    description: "The url path of the first page in the session - the page which the user lands on first."
    type: string
    sql: ${TABLE}.first_page_url_path ;;
  }


  dimension: gclid {
    type: string
    sql: ${TABLE}.gclid ;;
  }

    dimension: is_bounced_session {
      group_label: "Behavior"
      description: "A boolean field denoting whether the session is a 'bounced session' - this is when there are no events (clicks, plays etc) occurring within the session."
      type: yesno
      sql: ${TABLE}.is_bounced_session ;;
    }

    dimension: last_page_url {
      hidden: yes

      type: string
      sql: ${TABLE}.last_page_url ;;
    }

    dimension: last_page_url_host {
      hidden: yes

      type: string
      sql: ${TABLE}.last_page_url_host ;;
    }

    dimension: last_page_url_path {
      group_label: "Behavior"
      label: "Exit Page Path"
      description: "The url path of the last page in the session - the last page before the user leaves the site."
      hidden: no
      type: string
      sql: ${TABLE}.last_page_url_path ;;
    }

    dimension: mins_between_sessions {
      hidden: yes

      type: number
      sql: ${TABLE}.mins_between_sessions ;;
    }

  dimension: prev_session_channel {
    group_label: "Conversions"
    label: "Conversion Channel"
    description: "The channel through which the user visited the site for their previous session - the session before the current session. See 'Marketing Channel' field for its definition."
    type: string
    sql: ${TABLE}.prev_session_channel ;;
  }

  dimension: prev_utm_medium {
    group_label: "Conversions"
    label: "Conversion Medium"
    description: "The Medium through which the user visited the site for their previous session - the session before the current session. See 'Ad Medium' field for its definition."
    type: string
    sql: ${TABLE}.prev_utm_medium ;;
  }

  dimension: prev_utm_source {
    group_label: "Conversions"
    label: "Conversion Source"
    description: "The Medium through which the user visited the site for their previous session - the session before the current session. See 'Ad Medium' field for its definition."
    type: string
    sql: ${TABLE}.prev_utm_source ;;
  }

  dimension: referrer_host {
    group_label: "    Acquisition"
    label: "Referrer Host"
    description: ""
    type: string
    sql: ${TABLE}.referrer_host ;;
  }

  dimension: referrer_medium {
    group_label: "    Acquisition"
    label: "Referrer Medium"
    description: ""
    type: string
    sql: ${TABLE}.referrer_medium ;;
  }

  dimension: referrer_source {
    group_label: "    Acquisition"
    label: "Referrer Source"
    description: ""
    type: string
    sql: ${TABLE}.referrer_source ;;
  }

  dimension: search {
    group_label: "    Acquisition"
    label: "Search"
    hidden: yes
    type: string
    sql: ${TABLE}.search ;;
  }


  dimension_group: session_end_ts {
    group_label: "Dates"
    label: "Session End"
    description: ""
    type: time
    timeframes: [
      date
    ]
    sql: ${TABLE}.session_end_ts ;;
  }

  dimension: session_id {
    type: string
    hidden: yes
    sql: ${TABLE}.session_id ;;
  }

  dimension_group: session_start_ts {
    group_label: "Dates"
    label: "Session Start"
    description: ""
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
    sql: ${TABLE}.session_start_ts ;;
  }


   dimension: user_session_number {
    group_label: "  Audience"
    label: "User Session Number"
    description: ""
    type: number
    sql: ${TABLE}.user_session_number ;;
  }

  dimension: utm_campaign {
    group_label: "    Acquisition"
    label: "Ad Campaign"
    description: ""
    type: string
    sql: ${TABLE}.utm_campaign ;;
  }

  dimension: utm_content {
    group_label: "    Acquisition"
    label: "Ad Content"
    description: ""
    type: string
    sql: ${TABLE}.utm_content ;;
  }

  dimension: utm_medium {
    group_label: "    Acquisition"
    label: "Ad Medium"
    description: ""
    type: string
    sql: ${TABLE}.utm_medium ;;
  }

  dimension: utm_source {
    group_label: "    Acquisition"
    label: "Ad Source"
    description: ""
    type: string
    sql: ${TABLE}.utm_source ;;
  }

  dimension: utm_term {
    group_label: "    Acquisition"
    label: "Ad Keyword"
    description: ""
    type: string
    sql: ${TABLE}.utm_term ;;
  }

  dimension: visitor_id {
    hidden: yes
    type: string
    sql: ${TABLE}.visitor_id ;;
  }

  dimension: web_sessions_pk {
    type: string
    primary_key: yes
    hidden: yes
    sql: ${TABLE}.web_sessions_pk ;;
  }

  measure: total_web_sessions_pk {
    label: "Total Sessions"
    description: ""
    type: count_distinct
    sql: ${TABLE}.web_sessions_pk ;;
  }
}
