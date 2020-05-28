view: email_sends_dim {
  sql_table_name: `email_sends_dim`
    ;;

  dimension: campaign_archive_url {
    type: string
    sql: ${TABLE}.campaign_archive_url ;;
  }

  dimension: campaign_content_type {
    type: string
    sql: ${TABLE}.campaign_content_type ;;
  }

  dimension_group: campaign_created_at_ts {
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
    sql: ${TABLE}.campaign_created_at_ts ;;
  }

  dimension: campaign_is_resendable {
    type: yesno
    sql: ${TABLE}.campaign_is_resendable ;;
  }

  dimension: campaign_list_is_active {
    type: yesno
    sql: ${TABLE}.campaign_list_is_active ;;
  }

  dimension_group: campaign_sent_ts {
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
    sql: ${TABLE}.campaign_sent_ts ;;
  }

  dimension: campaign_status {
    type: string
    sql: ${TABLE}.campaign_status ;;
  }

  dimension: campaign_subject_line {
    type: string
    sql: ${TABLE}.campaign_subject_line ;;
  }

  dimension: campaign_title {
    type: string
    sql: ${TABLE}.campaign_title ;;
  }

  dimension: campaign_tracking_html_clicks {
    type: yesno
    sql: ${TABLE}.campaign_tracking_html_clicks ;;
  }

  dimension: campaign_tracking_opens {
    type: yesno
    sql: ${TABLE}.campaign_tracking_opens ;;
  }

  dimension: campaign_tracking_text_clicks {
    type: yesno
    sql: ${TABLE}.campaign_tracking_text_clicks ;;
  }

  dimension: click_rate_pct {
    type: number
    sql: ${TABLE}.click_rate_pct ;;
  }

  dimension: list_id {
    type: string
    sql: ${TABLE}.list_id ;;
  }

  dimension: list_name {
    type: string
    sql: ${TABLE}.list_name ;;
  }

  dimension: open_rate_pct {
    type: number
    sql: ${TABLE}.open_rate_pct ;;
  }

  dimension: send_id {
    type: string
    sql: ${TABLE}.send_id ;;
  }

  dimension: send_pk {
    primary_key: yes
    type: string
    sql: ${TABLE}.send_pk ;;
  }

  dimension: total_campaign_emails_sent {
    type: number
    sql: ${TABLE}.total_campaign_emails_sent ;;
  }

  dimension: total_clicks {
    type: number
    sql: ${TABLE}.total_clicks ;;
  }

  dimension: total_opens {
    type: number
    sql: ${TABLE}.total_opens ;;
  }

  dimension: total_recipient_count {
    type: number
    sql: ${TABLE}.total_recipient_count ;;
  }

  dimension: total_subscriber_clicks {
    type: number
    sql: ${TABLE}.total_subscriber_clicks ;;
  }

  dimension: total_unique_opens {
    type: number
    sql: ${TABLE}.total_unique_opens ;;
  }

  measure: count {
    type: count
    drill_fields: [list_name]
  }
}