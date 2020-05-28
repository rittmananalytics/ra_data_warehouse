view: contacts_dim {
  sql_table_name: `contacts_dim`
    ;;

  dimension: all_contact_addresses {
    hidden: yes
    sql: ${TABLE}.all_contact_addresses ;;
  }

  dimension: all_contact_company_ids {
    type: string
    sql: ${TABLE}.all_contact_company_ids ;;
  }

  dimension: all_contact_emails {
    type: string
    sql: ${TABLE}.all_contact_emails ;;
  }

  dimension: all_contact_ids {
    type: string
    sql: ${TABLE}.all_contact_ids ;;
  }

  dimension_group: contact_created {
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
    sql: ${TABLE}.contact_created_date ;;
  }

  dimension_group: contact_last_modified {
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
    sql: ${TABLE}.contact_last_modified_date ;;
  }

  dimension: contact_mobile_phone {
    type: string
    sql: ${TABLE}.contact_mobile_phone ;;
  }

  dimension: contact_name {
    type: string
    sql: ${TABLE}.contact_name ;;
  }

  dimension: contact_phone {
    type: string
    sql: ${TABLE}.contact_phone ;;
  }

  dimension: contact_pk {
    type: string
    sql: ${TABLE}.contact_pk ;;
  }

  dimension: job_title {
    type: string
    sql: ${TABLE}.job_title ;;
  }

  measure: count {
    type: count
    drill_fields: [contact_name]
  }
}

view: contacts_dim__all_contact_addresses {
  dimension: contact_address {
    type: string
    sql: ${TABLE}.contact_address ;;
  }

  dimension: contact_city {
    type: string
    sql: ${TABLE}.contact_city ;;
  }

  dimension: contact_country {
    type: string
    sql: ${TABLE}.contact_country ;;
  }

  dimension: contact_postcode_zip {
    type: string
    sql: ${TABLE}.contact_postcode_zip ;;
  }

  dimension: contact_state {
    type: string
    sql: ${TABLE}.contact_state ;;
  }
}