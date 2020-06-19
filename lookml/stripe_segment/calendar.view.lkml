view: calendar {
  derived_table: {
    sql_trigger_value: select current_date ;;
    indexes: ["cal_date"]
    distribution_style: all
    sql: SELECT
        DATEADD('days',
          (p0.n
          + p1.n*2
          + p2.n * POWER(2,2)
          + p3.n * POWER(2,3)
          + p4.n * POWER(2,4)
          + p5.n * POWER(2,5)
          + p6.n * POWER(2,6)
          + p7.n * POWER(2,7)
          + p8.n * POWER(2,8)
          + p9.n * POWER(2,9)
          + p10.n * POWER(2,10)
          )::int
          , '2012-11-01'::date
          )
          as cal_date
        FROM
          (SELECT 0 as n UNION SELECT 1) p0,
          (SELECT 0 as n UNION SELECT 1) p1,
          (SELECT 0 as n UNION SELECT 1) p2,
          (SELECT 0 as n UNION SELECT 1) p3,
          (SELECT 0 as n UNION SELECT 1) p4,
          (SELECT 0 as n UNION SELECT 1) p5,
          (SELECT 0 as n UNION SELECT 1) p6,
          (SELECT 0 as n UNION SELECT 1) p7,
          (SELECT 0 as n UNION SELECT 1) p8,
          (SELECT 0 as n UNION SELECT 1) p9,
          (SELECT 0 as n UNION SELECT 1) p10
        WHERE
        DATEADD('days',
          (p0.n
          + p1.n*2
          + p2.n * POWER(2,2)
          + p3.n * POWER(2,3)
          + p4.n * POWER(2,4)
          + p5.n * POWER(2,5)
          + p6.n * POWER(2,6)
          + p7.n * POWER(2,7)
          + p8.n * POWER(2,8)
          + p9.n * POWER(2,9)
          + p10.n * POWER(2,10)
          )::int
          , '2012-11-01'::date
          ) <= current_date
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
