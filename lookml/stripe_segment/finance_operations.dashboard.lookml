- dashboard: finance_operations
  title: Finance Operations
  layout: grid
  rows:
    - elements: [add_a_unique_name_1462199027307, add_a_unique_name_1462199030365, add_a_unique_name_1462199034110]
      height: 200
    - elements: [add_a_unique_name_1462199022945]
      height: 400
    - elements: [add_a_unique_name_1462199037544, add_a_unique_name_1462199046966, add_a_unique_name_1462199049740, add_a_unique_name_1462199071518, add_a_unique_name_1462199076746]
      height: 200
    - elements: [add_a_unique_name_1462199080404]
      height: 400
    - elements: [add_a_unique_name_1462199084068 ,add_a_unique_name_1462199093780]
      height: 400
    - elements: [add_a_unique_name_1462199096968]
      height: 400
    - elements: [add_a_unique_name_1462199100492]
      height: 400

  filters:
  - name: charge_date
    title: "Visit Date"
    type: date_filter
    default_value: 90 days ago for 45 days #change default filter value

  elements:

  - name: add_a_unique_name_1462199022945
    title: Charge Failure Reason
    type: looker_bar
    model: segment_stripe
    explore: calendar
    dimensions: [charges.failure_code]
    measures: [charges.charge_count, charges.total_gross_amount]
    filters:
      charges.failure_code: -NULL
      charges.status: failed
    listen:
      charge_date: calendar.cal_date_month
    sorts: [charges.charge_count desc]
    limit: 500
    column_limit: 50
    stacking: ''
    colors: ['#a6cee3', '#1f78b4', '#b2df8a', '#33a02c', '#fb9a99', '#e31a1c', '#fdbf6f',
      '#ff7f00', '#cab2d6', '#6a3d9a', '#edbc0e', '#b15928']
    show_value_labels: true
    label_density: 14
    legend_position: right
    x_axis_gridlines: false
    show_view_names: false
    limit_displayed_rows: false
    y_axis_combined: false
    show_y_axis_labels: false
    show_y_axis_ticks: false
    y_axis_tick_density: default
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    show_null_labels: false

  - name: add_a_unique_name_1462199027307
    title: Total Charges
    type: single_value
    model: segment_stripe
    explore: calendar
    listen:
      charge_date: calendar.cal_date_month
    measures: [charges.charge_count]
    sorts: [charges.charge_count desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462199030365
    title: Total Failed Charges Amount
    type: single_value
    model: segment_stripe
    explore: calendar
    listen:
      charge_date: calendar.cal_date_month
    measures: [charges.charge_count]
    filters:
      charges.status: failed
    sorts: [charges.charge_count desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462199034110
    title: Total Refund Count
    type: single_value
    model: segment_stripe
    listen:
      charge_date: calendar.cal_date_month
    explore: calendar
    measures: [charges.refund_count]
    sorts: [charges.refund_count desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462199037544
    title: Total Gross Charges
    type: single_value
    model: segment_stripe
    explore: calendar
    listen:
      charge_date: calendar.cal_date_month
    measures: [charges.total_gross_amount]
    sorts: [charges.total_gross_amount desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462199046966
    title: Total Failed Charges
    type: single_value
    model: segment_stripe
    explore: calendar
    listen:
      charge_date: calendar.cal_date_month
    measures: [charges.total_failed_charges]
    sorts: [charges.total_failed_charges desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462199049740
    title: Total Net Charges
    type: single_value
    model: segment_stripe
    explore: calendar
    listen:
      charge_date: calendar.cal_date_month
    measures: [charges.total_net_amount]
    sorts: [charges.total_net_amount desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462199071518
    title: Delinquent Customer Count
    type: single_value
    listen:
      charge_date: calendar.cal_date_month
    model: segment_stripe
    explore: calendar
    measures: [customers.count]
    filters:
      customers.delinquent: 'Yes'
    sorts: [invoices.attempt_count, plans.name, customers.count desc]
    limit: 500
    column_limit: 50
    font_size: small
    text_color: '#49719a'

  - name: add_a_unique_name_1462199076746
    title: Average Days to Payment Received
    type: single_value
    model: segment_stripe
    listen:
      charge_date: calendar.cal_date_month
    explore: calendar
    measures: [charges.avg_days_until_received]
    sorts: [charges.outstanding_charge_time desc, charges.avg_days_until_received desc]
    limit: 500
    font_size: small
    text_color: black

  - name: add_a_unique_name_1462199080404
    title: Invoice Attempt Count
    type: looker_column
    model: segment_stripe
    explore: calendar
    listen:
      charge_date: calendar.cal_date_month
    dimensions: [invoices.attempt_count]
    measures: [invoices.count]
    filters:
      invoices.attempt_count: NOT NULL
    sorts: [invoices.attempt_count]
    limit: 500
    column_limit: 50
    stacking: ''
    colors: ['#62bad4', '#a9c574', '#929292', '#9fdee0', '#1f3e5a', '#90c8ae', '#92818d',
      '#c5c6a6', '#82c2ca', '#cee0a0', '#928fb4', '#9fc190']
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    limit_displayed_rows: false
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: ordinal
    y_axis_scale_mode: log
    show_null_labels: false

  - name: add_a_unique_name_1462199084068
    title: Customers with Unpaid Invoices
    type: table
    model: segment_stripe
    explore: calendar
    listen:
      charge_date: calendar.cal_date_month
    dimensions: [customers.email]
    measures: [invoices.total_amount_due]
    filters:
      customers.email: -NULL
      invoices.paid: 'No'
      invoices.total_amount_due: '>0'
    sorts: [invoices.total_amount_due desc]
    limit: 500
    column_limit: 50
    show_view_names: false
    show_row_numbers: false
    truncate_column_names: false
    table_theme: editable
    limit_displayed_rows: false

  - name: add_a_unique_name_1462199093780
    title: Top Customers by Total Spend
    type: table
    model: segment_stripe
    explore: calendar
    dimensions: [customers.email]
    measures: [charges.total_net_amount]
    filters:
      customers.email: -NULL
    listen:
      charge_date: calendar.cal_date_month
    sorts: [charges.total_net_amount desc]
    limit: 500
    column_limit: 50
    show_view_names: false
    show_row_numbers: true
    truncate_column_names: false
    table_theme: editable
    limit_displayed_rows: false

  - name: add_a_unique_name_1462199096968
    title: Payment Received Time by Charge Date
    type: looker_line
    model: segment_stripe
    explore: calendar
    listen:
      charge_date: calendar.cal_date_month
    dimensions: [calendar.cal_date_date]
    measures: [charges.avg_days_until_received]
    sorts: [calendar.cal_date_date desc]
    limit: 500
    column_limit: 50
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: true
    limit_displayed_rows: false
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    show_null_points: false
    point_style: none
    interpolation: monotone

  - name: add_a_unique_name_1462199100492
    title: Gross Charges vs Failures/Refunds Over Time
    type: looker_column
    model: segment_stripe
    explore: calendar
    dimensions: [calendar.cal_date_date]
    measures: [charges.total_gross_amount, charges.total_net_amount, charges.total_failed_charges,
      charges.total_refunds]
    dynamic_fields:
    - table_calculation: failed_charges
      label: Failed Charges
      expression: -1 * ${charges.total_failed_charges}
      value_format_name: usd
    - table_calculation: refunds
      label: Refunds
      expression: -1 * ${charges.total_refunds}
      value_format_name: usd
    listen:
      charge_date: calendar.cal_date_month
    hidden_fields: [charges.total_net_amount, charges.total_failed_charges, charges.total_refunds]
    sorts: [calendar.cal_date_date]
    limit: 500
    stacking: ''
    colors: ['#94cf78', '#bf7d75', '#0c0d0c', '#33a02c', '#fb9a99', '#e31a1c', '#fdbf6f',
      '#ff7f00', '#cab2d6', '#6a3d9a', '#edbc0e', '#b15928']
    show_value_labels: false
    label_density: 2
    font_size: small
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    limit_displayed_rows: false
    hidden_series: [charges.total_refunds]
    y_axis_combined: true
    show_y_axis_labels: false
    show_y_axis_ticks: true
    y_axis_tick_density: default
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: time
    y_axis_scale_mode: linear
    show_null_labels: false
