connection: "ra_dw_prod"

# include all the views
include: "/lookml/views/**/*.view"

datagroup: ra_warehouse_lookml_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: ra_warehouse_lookml_default_datagroup



explore: companies_dim {
  label: "Customers"

  join: delivery_projects_dim {
    view_label: "Delivery Projects"
    type: left_outer
    sql_on: ${companies_dim.company_pk} = ${delivery_projects_dim.company_pk}  ;;
    relationship: many_to_one
  }
  join: delivery_tasks_dim {
    view_label: "Delivery Tasks"
    type: left_outer
    sql_on: ${delivery_projects_dim.project_id} = ${delivery_tasks_dim.project_id} ;;
    relationship: many_to_one

  }
  join: timesheet_projects_dim {
    view_label: "Timesheet Projects"
    type: left_outer
    sql_on: ${companies_dim.company_pk} = ${timesheet_projects_dim.company_pk}  ;;
    relationship: many_to_one
  }
  join: timesheets_fact {
    view_label: "Timesheets"
    type: left_outer
    sql_on: ${companies_dim.company_pk} = ${timesheets_fact.company_pk}
        and ${timesheets_fact.timesheet_project_pk} = ${timesheet_projects_dim.timesheet_project_pk};;
    relationship: one_to_many
  }
  join: timesheet_tasks_dim {
    view_label: "Timesheet Tasks"
    type: inner
    sql_on: ${timesheets_fact.timesheet_task_pk} = ${timesheet_tasks_dim.timesheet_task_pk} ;;
    relationship: many_to_one
  }
  join: timesheet_invoices_fact {
    from: invoices_fact
    view_label: "Timesheet Invoices"
    relationship: many_to_one
    type: left_outer
    sql_on: ${timesheet_invoices_fact.company_pk} = ${companies_dim.company_pk};;
  }
  join: deals_fact {
    view_label: "Sales Deals"
    relationship: many_to_one
    type: left_outer
    sql_on: ${deals_fact.company_pk} = ${companies_dim.company_pk};;
  }

}

explore: ad_campaigns_dim {
  label: "Advertising"
  join: adsets_dim {
    sql_on: ${ad_campaigns_dim.campaign_id} = ${adsets_dim.campaign_id};;
    type: left_outer
    relationship: one_to_many
  }
  join: ads_dim {
    sql_on: ${ad_campaigns_dim.campaign_id} = ${ads_dim.campaign_id} and ${ads_dim.adset_id} = ${adsets_dim.adset_id};;
    type: left_outer
    relationship: one_to_many
  }

}

explore: profile_wh_tables {
  label: "Data Profiler"
}

explore: audit_dbt_last_results {
  label: "ETL Job Status"
}


explore: web_sessions_fact {
  label: "Web Analytics"
  view_label: "Sessions"
  join: web_pageviews_fact {
    view_label: "Page Views"
    sql: ${web_sessions_fact.session_id} = ${web_pageviews_fact.session_id} ;;
    relationship: one_to_many
  }
}

explore: email_send_outcomes_fact {
  join: email_lists_dim {
    view_label: "Email Lists"
    sql_on: ${email_send_outcomes_fact.list_pk} = ${email_lists_dim.list_pk} ;;
    relationship: many_to_one
    type: inner
  }
  join: contacts_dim {
    view_label: "Contacts"
    sql_on: ${email_send_outcomes_fact.contact_pk} = ${contacts_dim.contact_pk} ;;
    relationship: many_to_one
  }
  join: email_sends_dim {
    view_label: "Email Sends"
    sql_on: ${email_send_outcomes_fact.send_pk} = ${email_sends_dim.send_pk} ;;
    type: inner
    relationship: many_to_one
  }
}
