view: delivery_projects_dim {
  sql_table_name: `mark_bi_apps_dev.delivery_projects_dim`
    ;;

  dimension: company_pk {
    type: string
    sql: ${TABLE}.company_pk ;;
  }

  dimension: delivery_project_pk {
    type: string
    primary_key: yes
    sql: ${TABLE}.delivery_project_pk ;;
  }

  dimension: lead_user_id {
    type: string
    sql: ${TABLE}.lead_user_id ;;
  }

  dimension: project_category_description {
    type: string
    sql: ${TABLE}.project_category_description ;;
  }

  dimension: project_category_name {
    type: string
    sql: ${TABLE}.project_category_name ;;
  }

  dimension_group: project_created_at_ts {
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
    sql: ${TABLE}.project_created_at_ts ;;
  }

  dimension: project_id {
    type: string
    sql: ${TABLE}.project_id ;;
  }

  dimension_group: project_modified_at_ts {
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
    sql: ${TABLE}.project_modified_at_ts ;;
  }

  dimension: project_name {
    type: string
    sql: ${TABLE}.project_name ;;
  }

  dimension: project_notes {
    type: string
    sql: ${TABLE}.project_notes ;;
  }

  dimension: project_status {
    type: string
    sql: ${TABLE}.project_status ;;
  }

  dimension: project_type {
    type: string
    sql: ${TABLE}.project_type ;;
  }

  measure: count {
    type: count
    drill_fields: [project_category_name, project_name]
  }
}
