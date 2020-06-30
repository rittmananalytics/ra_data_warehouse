view: calendar {
  derived_table: {
    sql_trigger_value: select current_date ;;
    indexes: ["cal_date"]
    distribution_style: all
    sql: SELECT
  timestamp(example) as cal_date
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2012-01-01', current_date, INTERVAL 1 DAY)) AS example
       ;;
  }

  dimension_group: cal_date {
    type: time
    timeframes: [
      year,
      month,
      date,
      day_of_week,
      month_num,
      day_of_week_index,
      quarter,
      quarter_of_year,
      week,
      week_of_year
    ]
    sql: ${TABLE}.cal_date ;;
  }
}
