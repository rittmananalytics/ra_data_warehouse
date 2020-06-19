- dashboard: executive_summary
  title: Executive Summary
  layout: grid
  rows:
    - elements: [add_a_unique_name_1462169894754, add_a_unique_name_1462169986635, add_a_unique_name_1462170018213, add_a_unique_name_1462170265666, add_a_unique_name_1462169944434]
      height: 200
    - elements: [add_a_unique_name_1462170071843, add_a_unique_name_1462170103121, add_a_unique_name_1462170131608, add_a_unique_name_1462170158647, add_a_unique_name_1462170192768, add_a_unique_name_1462170234178]
      height: 200
    - elements: [add_a_unique_name_1462170298899, add_a_unique_name_1462170478391]
      height: 400
    - elements: [add_a_unique_name_1462170327278]
      height: 400

  filters:
  - name: charge_date
    title: "Visit Date"
    type: date_filter
    default_value: 90 days ago for 45 days # change default filter value

  elements:

  - name: add_a_unique_name_1462169894754
    title: Total Gross Charges
    type: single_value
    model: segment_stripe
    explore: calendar
    measures: [charges.total_gross_amount]
    sorts: [charges.total_gross_amount desc]
    limit: 500
    listen:
      charge_date: calendar.cal_date_month
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462169944434
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

  - name: add_a_unique_name_1462169986635
    title: Total Refunded Charges
    type: single_value
    model: segment_stripe
    explore: calendar
    listen:
      charge_date: calendar.cal_date_month
    measures: [charges.total_refunds]
    sorts: [charges.total_refunds desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462170018213
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

  - name: add_a_unique_name_1462170071843
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

  - name: add_a_unique_name_1462170103121
    title: Total Failed Charges Count
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

  - name: add_a_unique_name_1462170131608
    title: Total Refunded Count
    type: single_value
    model: segment_stripe
    explore: calendar
    listen:
      charge_date: calendar.cal_date_month
    measures: [charges.refund_count]
    sorts: [charges.refund_count desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462170158647
    title: Total Unpaid Invoices Count
    type: single_value
    model: segment_stripe
    listen:
      charge_date: calendar.cal_date_month
    explore: calendar
    measures: [invoices.count]
    filters:
      invoices.paid: 'No'
    sorts: [invoices.count desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462170192768
    title: Total Unpaid Invoices Amount
    type: single_value
    model: segment_stripe
    explore: calendar
    listen:
      charge_date: calendar.cal_date_month
    measures: [invoices.total_amount_due]
    filters:
      invoices.paid: 'No'
    sorts: [invoices.total_amount_due desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462170234178
    title: Total New Customers
    type: single_value
    model: segment_stripe
    explore: customer
    listen:
      charge_date: customers.created_date
    measures: [customers.count]
    sorts: [customers.count desc]
    limit: 500
    font_size: small
    text_color: '#49719a'

  - name: add_a_unique_name_1462170265666
    title: Average Days to Payment Received
    type: single_value
    model: segment_stripe
    explore: calendar
    listen:
      charge_date: calendar.cal_date_month
    measures: [charges.avg_days_until_received]
    sorts: [charges.outstanding_charge_time desc, charges.avg_days_until_received desc]
    limit: 500
    font_size: small
    text_color: black

  - name: add_a_unique_name_1462170298899
    title: Charge Status
    type: looker_pie
    model: segment_stripe
    explore: calendar
    dimensions: [charges.status]
    measures: [charges.charge_count]
    filters:
      charges.status: -NULL
    sorts: [invoices.total_amount_due desc]
    limit: 500
    listen:
      charge_date: calendar.cal_date_month
    column_limit: 50
    value_labels: labels
    label_type: labPer
    colors: ['#94cf78', '#bf7d75', '#929292', '#9fdee0', '#1f3e5a', '#90c8ae', '#92818d',
      '#c5c6a6', '#82c2ca', '#cee0a0', '#928fb4', '#9fc190']
    inner_radius: 50
    show_view_names: false

  - name: add_a_unique_name_1462170327278
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
    hidden_fields: [charges.total_net_amount, charges.total_failed_charges, charges.total_refunds]
    listen:
      charge_date: calendar.cal_date_month
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

  - name: add_a_unique_name_1462170478391
    title: Charges Over Time
    type: looker_column
    model: segment_stripe
    explore: calendar
    dimensions: [calendar.cal_date_date]
    measures: [charges.total_net_amount, charges.charge_count]
    listen:
      charge_date: calendar.cal_date_month
    sorts: [calendar.cal_date_date]
    limit: 500
    stacking: ''
    colors: ['#a6cee3', '#1f78b4', '#b2df8a', '#33a02c', '#fb9a99', '#e31a1c', '#fdbf6f',
      '#ff7f00', '#cab2d6', '#6a3d9a', '#edbc0e', '#b15928']
    show_value_labels: false
    label_density: 2
    font_size: small
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    series_types:
      charges.total_net_amount: line
    limit_displayed_rows: false
    y_axis_combined: false
    show_y_axis_labels: false
    show_y_axis_ticks: true
    y_axis_tick_density: default
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    show_null_labels: false
