- dashboard: customer_lookup
  title: Customer Lookup
  layout: grid
  rows:
    - elements: [add_a_unique_name_1462199859857,add_a_unique_name_1462199864893,add_a_unique_name_1462199867389]
      height: 200
    - elements: [add_a_unique_name_1462199869537, add_a_unique_name_1462199871781, add_a_unique_name_1462199873959 ]
      height: 200
    - elements: [add_a_unique_name_1462199876088, add_a_unique_name_1462199878233]
      height: 200
    - elements: [add_a_unique_name_1462199880688]
      height: 400
    - elements: [add_a_unique_name_1462199883129]
      height: 400

  filters:

  - name: customer_email
    title: "Customer Email"
    type: field_filter
    explore: customer
    field: customers.email
    default_value: 8624702d9c382858919e0b9005e85b64cfa2bc92@gmail.com # change filter value if applicable

  elements:

  - name: add_a_unique_name_1462199859857
    title: Customer Detail
    type: looker_single_record
    model: segment_stripe
    explore: customer
    listen:
      customer_email: customers.email
    dimensions: [customers.email, customers.created_date, customers.delinquent, customers.currency]
    sorts: [customers.email]
    limit: 1
    show_view_names: false

  - name: add_a_unique_name_1462199864893
    title: Total Gross Charges
    type: single_value
    model: segment_stripe
    explore: calendar
    listen:
      customer_email: customers.email
    measures: [charges.total_gross_amount]
    sorts: [charges.total_gross_amount desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462199867389
    title: Total Net Charges
    type: single_value
    model: segment_stripe
    listen:
      customer_email: customers.email
    explore: calendar
    measures: [charges.total_net_amount]
    sorts: [charges.total_net_amount desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462199869537
    title: Total Charges
    type: single_value
    model: segment_stripe
    listen:
      customer_email: customers.email
    explore: calendar
    measures: [charges.charge_count]
    sorts: [charges.charge_count desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462199871781
    title: Total Refund Count
    type: single_value
    model: segment_stripe
    listen:
      customer_email: customers.email
    explore: calendar
    measures: [charges.refund_count]
    sorts: [charges.refund_count desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462199873959
    title: Total Failed Charges Count
    type: single_value
    model: segment_stripe
    listen:
      customer_email: customers.email
    explore: calendar
    measures: [charges.charge_count]
    filters:
      charges.status: failed
    sorts: [charges.charge_count desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462199876088
    title: Total Failed Charges
    type: single_value
    model: segment_stripe
    listen:
      customer_email: customers.email
    explore: calendar
    measures: [charges.total_failed_charges]
    sorts: [charges.total_failed_charges desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462199878233
    title: Total Refunded Charges
    type: single_value
    model: segment_stripe
    listen:
      customer_email: customers.email
    explore: calendar
    measures: [charges.total_refunds]
    sorts: [charges.total_refunds desc]
    limit: 500
    font_size: small
    value_format: ''
    text_color: black

  - name: add_a_unique_name_1462199880688
    title: Customer Invoices
    type: table
    model: segment_stripe
    listen:
      customer_email: customers.email
    explore: calendar
    dimensions: [invoices.id, invoices.date_date, invoices.closed, invoices.paid, invoices.attempt_count]
    measures: [invoices.total_amount_due]
    sorts: [invoices.paid, invoices.date_date]
    limit: 500
    show_view_names: false
    show_row_numbers: false
    truncate_column_names: false
    table_theme: white
    limit_displayed_rows: false

  - name: add_a_unique_name_1462199883129
    title: Charges Over Time
    type: looker_column
    model: segment_stripe
    listen:
      customer_email: customers.email
    explore: calendar
    dimensions: [calendar.cal_date_date]
    measures: [charges.total_net_amount, charges.charge_count]
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
