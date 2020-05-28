view: users_dim {
  sql_table_name: `users_dim`
    ;;

  dimension: all_user_emails {
    type: string
    sql: ${TABLE}.all_user_emails ;;
  }

  dimension: all_user_ids {
    type: string
    sql: ${TABLE}.all_user_ids ;;
  }

  dimension: user_cost_rate {
    type: number
    sql: ${TABLE}.user_cost_rate ;;
  }

  dimension_group: user_created_ts {
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
    sql: ${TABLE}.user_created_ts ;;
  }

  dimension: user_default_hourly_rate {
    type: number
    sql: ${TABLE}.user_default_hourly_rate ;;
  }

  dimension: user_is_active {
    type: yesno
    sql: ${TABLE}.user_is_active ;;
  }

  dimension: user_is_contractor {
    type: yesno
    sql: ${TABLE}.user_is_contractor ;;
  }

  dimension: user_is_staff {
    type: yesno
    sql: ${TABLE}.user_is_staff ;;
  }

  dimension_group: user_last_modified_ts {
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
    sql: ${TABLE}.user_last_modified_ts ;;
  }

  dimension: user_name {
    type: string
    sql: ${TABLE}.user_name ;;
  }

  dimension: user_phone {
    type: string
    sql: ${TABLE}.user_phone ;;
  }

  dimension: user_pk {
    type: string
    sql: ${TABLE}.user_pk ;;
  }

  dimension: user_weekly_capacity {
    type: number
    sql: ${TABLE}.user_weekly_capacity ;;
  }

  measure: count {
    type: count
    drill_fields: [user_name]
  }
}