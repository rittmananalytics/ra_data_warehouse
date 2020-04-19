# Rittman Analytics Business Intelligence Applications

## Introduction
This dbt package contains a set of pre-build, pre-integrated Load and Transform dbt models for common SaaS applications.
Objectives for this package are as follows:

1. To standardise how we source and model SaaS data sources in-general
2. To make it simpler to run data quality tests than to not, by defining these tests in-advance
3. To enable merging of customer, product, contact and other shared entity data with no single authoratitive source
4. To pre-create derived analytics measures for individual and combinations of sources
5. In-time, to create a means of selecting sources or subject areas ("modules") and have just those sources/modules loaded (and deployed for a customer)
6. To do all of this in a way that embraces, rather than avoids, community additions to these sources and derived analytics models

* [Design Approach and Coding Standards](https://github.com/rittmananalytics/ra_bi_apps/wiki/BI-Apps-Design-Documentation)

### Current Modules

| Module       | Implemented Sources | Planned Sources             |
|--------------|---------------------|-----------------------------|
| CRM & Sales  | Hubspot CRM         | Salesforce CRM              |
| Projects     | Harvest Timesheets, Asana, Jira  | Netsuite Projects           |
| Finance      | Xero, Stripe                | Chargebee, Freshbooks                    |
| Marketing    | Mailchimp           | Salesforce Marketing Cloud, Hubspot Email Marketing  |
| Events + Web | Segment             | Mixpanel, Google Analytics (Free), Qubit Live Tap                   |

#### Pre-Integrated Conformed Dimensional Model

Customers, contacts, projects and other shared dimensions are automatically created from all data sources, deduplicating by name and merge lookup files using a process that preserves source system keys whilst assigning a unique ID for each customer, contact etc.

#### Data sources implemented for a particular customer can be selected in dbt_project.yml config file

```
vars:
    enable_harvest_projects_source:      true
    enable_hubspot_crm_source:           true
    enable_asana_projects_source:        true
    enable_jira_projects_source:         true
    enable_stripe_payments_source:       true
    enable_xero_accounting_source:       true
    enable_mailchimp_email_source:       true
    enable_segment_events_source:        true
    enable_crm_warehouse:                true
    enable_finance_warehouse:            true
    enable_projects_warehouse:           true
    enable_marketing_warehouse:          true
```
#### Split between Source Extract and Transform, Integrate and Deduplicate and Warehouse Load Models

![STA, INT and WH ](https://github.com/rittmananalytics/ra_bi_apps/raw/master/ra_biapps/img/t_wh_diagram.png)

#### All transformation models and seed files deployed in separate datasets to main dimensional model tables

```
models:
  ra_bi_apps:
      # Applies to all files under models/example/
      t_adapters:
          materialized: view
          schema: staging
      wh_marts:
          materialized: table
seeds:
  ra_bi_apps:
      schema: seed_data
```
#### Predefined Data Quality tests on sources and warehouse tables

```
  - name: t_asana_projects_projects
    description: "Asana Delivery Projects"
    columns:
      - name: project_id
        tests:
          - unique
          - not_null
      - name: lead_user_id
        tests:
          - not_null:
              severity: warn
          - relationships:
              to: ref('t_asana_projects_users')
              field: user_id

  - name: wh_timesheets_fact
    description: "Projects Dimension"
    columns:
      - name: timesheet_pk
        tests:
          - unique
          - not_null
      - name: company_pk
        tests:
          - not_null
          - relationships:
              to: ref('wh_companies_dim')
              field: company_pk
```
