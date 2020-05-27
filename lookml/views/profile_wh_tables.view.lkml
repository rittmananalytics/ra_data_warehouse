view: profile_wh_tables {
  sql_table_name: `ra-development.analytics_dev_logs.profile_wh_tables`
    ;;

  dimension: avg_value {
    group_label: "Column Values"
    type: number
    sql: ${TABLE}._avg_value ;;
  }

  dimension: _avg_length {
    group_label: "Column Definition"

    type: number
    sql: ${TABLE}._avr_length ;;
  }

  dimension: count_distinct_values {
    group_label: "Column Values"

    type: number
    sql: ${TABLE}._distinct_values ;;
  }

  dimension: max_length {
    group_label: "Column Values"

    type: number
    sql: ${TABLE}._max_length ;;
  }

  dimension: max_value {
    group_label: "Column Values"

    type: string
    sql: ${TABLE}._max_value ;;
  }

  dimension: min_length {
    group_label: "Column Definition"

    type: number
    sql: ${TABLE}._min_length ;;
  }

  dimension: min_value {
    group_label: "Column Values"

    type: string
    sql: ${TABLE}._min_value ;;
  }

  dimension: _most_frequent_value {
    hidden: yes
    sql: ${TABLE}._most_frequent_value ;;
  }

  dimension: count_non_nulls {
    group_label: "Column Constraints"


    type: number
    sql: ${TABLE}._non_nulls ;;
  }

  dimension: count_nulls {
    group_label: "Column Constraints"

    type: number
    sql: ${TABLE}._nulls ;;
  }

  dimension: clustering_ordinal_position {
    group_label: "Object Details"

    type: number
    sql: ${TABLE}.clustering_ordinal_position ;;
  }

  dimension: column_name {
    group_label: "Object Details"

    type: string
    sql: ${TABLE}.column_name ;;
  }

  dimension: data_type {
    group_label: "Column Definition"

    type: string
    sql: ${TABLE}.data_type ;;
  }

  dimension: is_hidden {
    group_label: "Column Definition"

    type: yesno
    sql: ${TABLE}.is_hidden ;;
  }

  dimension: is_nullable {
    group_label: "Column Constraints"

    type: string
    sql: case when ${TABLE}.is_nullable = 'YES' then 'NOT NULL' else 'NULL' end;;
  }

  dimension: is_partitioning_column {
    group_label: "Column Constraints"

    type: yesno
    sql: ${TABLE}.is_partitioning_column ;;
  }

  dimension: is_recommended_nullable_compliant {
    group_label: "Column Constraints"

    type: yesno
    sql: ${TABLE}.is_recommended_nullable_compliant ;;
  }

  dimension: is_recommended_unique_key_compliant {
    group_label: "Column Constraints"

    type: yesno
    sql: ${TABLE}.is_recommended_unique_key_compliant ;;
  }

  dimension: is_system_defined {
    group_label: "Column Definition"

    type: yesno
    sql: ${TABLE}.is_system_defined ;;
  }

  dimension: ordinal_position {
    group_label: "Column Definition"

    type: number
    sql: ${TABLE}.ordinal_position ;;
  }

  dimension: pct_null {
    group_label: "Column Constraints"
    value_format_name: percent_2

    type: number
    sql: ${TABLE}.pct_null/100 ;;
  }

  dimension: pct_unique {
    group_label: "Column Constraints"
    value_format_name: percent_2
    type: number
    sql: ${TABLE}.pct_unique/100 ;;
  }

  dimension: recommended_nullable {
    group_label: "Column Constraints"

    type: string
    sql: ${TABLE}.recommended_nullable ;;
  }

  dimension: recommended_unique_key {
    group_label: "Column Constraints"

    type: string
    sql: ${TABLE}.recommended_unique_key ;;
  }

  dimension: table_catalog {
    group_label: "Object Details"

    type: string
    sql: ${TABLE}.table_catalog ;;
  }

  dimension: table_name {
    group_label: "Object Details"

    type: string
    sql: ${TABLE}.table_name ;;
  }

  dimension: table_rows {
    group_label: "Column Values"


    type: number
    sql: ${TABLE}.table_rows ;;
  }

  dimension: table_schema {
    group_label: "Object Details"

    type: string
    sql: ${TABLE}.table_schema ;;
  }


}

view: profile_wh_tables___most_frequent_value {
    dimension: count {
      group_label: "Column Values"

    type: number
    sql: ${TABLE}.count ;;
  }

  dimension: value {
    group_label: "Column Values"

    type: string
    sql: ${TABLE}.value ;;
  }
}
