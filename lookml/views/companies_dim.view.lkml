view: companies_dim {
  sql_table_name: `companies_dim`
    ;;

  dimension: all_company_addresses {
    hidden: yes
    sql: ${TABLE}.all_company_addresses ;;
  }

  dimension: all_company_ids {
    type: string
    sql: ${TABLE}.all_company_ids ;;
  }

  dimension_group: company_created {
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
    sql: ${TABLE}.company_created_date ;;
  }

  dimension: company_description {
    type: string
    sql: ${TABLE}.company_description ;;
  }

  dimension: company_finance_status {
    type: string
    sql: ${TABLE}.company_finance_status ;;
  }

  dimension: company_industry {
    type: string
    sql: ${TABLE}.company_industry ;;
  }

  dimension_group: company_last_modified {
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
    sql: ${TABLE}.company_last_modified_date ;;
  }

  dimension: company_linkedin_bio {
    type: string
    sql: ${TABLE}.company_linkedin_bio ;;
  }

  dimension: company_linkedin_company_page {
    type: string
    sql: ${TABLE}.company_linkedin_company_page ;;
  }

  dimension: company_name {
    type: string
    sql: ${TABLE}.company_name ;;
  }

  dimension: company_phone {
    type: string
    sql: ${TABLE}.company_phone ;;
  }

  dimension: company_pk {
    type: string
    sql: ${TABLE}.company_pk ;;
  }

  dimension: company_twitterhandle {
    type: string
    sql: ${TABLE}.company_twitterhandle ;;
  }

  dimension: company_website {
    type: string
    sql: ${TABLE}.company_website ;;
  }

  measure: count {
    type: count
    drill_fields: [company_name]
  }
}

view: companies_dim__all_company_addresses {
  dimension: company_address {
    type: string
    sql: ${TABLE}.company_address ;;
  }

  dimension: company_address2 {
    type: string
    sql: ${TABLE}.company_address2 ;;
  }

  dimension: company_city {
    type: string
    sql: ${TABLE}.company_city ;;
  }

  dimension: company_country {
    type: string
    sql: ${TABLE}.company_country ;;
  }

  dimension: company_state {
    type: string
    sql: ${TABLE}.company_state ;;
  }

  dimension: company_zip {
    type: string
    sql: ${TABLE}.company_zip ;;
  }
}