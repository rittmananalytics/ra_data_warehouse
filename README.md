## Introduction

The RA Data Warehouse is a framework for ingesting, combining and restructuring data from multiple source systems into a conformed, dimensional data warehouse. The framework is based around dbt ("Data Build Tool"), open-source toolkit for templating and orchestrating SQL-based data transformations of RAW API-sourced data into structures suitable for analysis, and pre-built transformations and design patterns taken from Rittman Analytics' previous data warehousing consulting experience.

dbt is a toolkit that solves the problem of testing, repeatability and modularity of analysts code by bringing the principles of modern software development to the analysts' workflow. The RA Development framework solves the problem of how to design your dbt transformations so that your project doesn't grind to a halt after you integrate your second, third, fourth data source because you need to combine identity across multiple systems, deduplicate multiple sources of customer data and make sure that numbers coming out of your BI tool still match with the numbers in your source systems.

## Design Goals

1. To standardise how we source and model SaaS data sources in-general
2. To make it simpler to run data quality tests than to not, by defining these tests in-advance
3. To enable merging of customer, product, contact and other shared entity data with no single authoratitive source
4. To pre-create derived analytics measures for individual and combinations of sources
5. To create a means of selecting sources or subject areas ("modules") and have just those sources/modules loaded (and deployed for a customer)
6. To enable use of either Stitch, Fivetran or Segment as the pipeline technology based on client need
7. To enable loading and integration of custom (customer app database) sources into the warehouse

### Current Dimensional Model

![Dimensional Model](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/dimensional_model.png)

## What are the Layers in the Warehouse Data Model?

![Model Layers](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/data_flow.png)

See [Design and Coding Approach](https://github.com/rittmananalytics/ra_data_warehouse/wiki/Design-and-Coding-Approach) for Implementation Details

## What Data Warehouse, Data Pipeline and Data Collection Technologies are Supported?

* Google BigQuery (Standard SQL)
* Stitch
* Fivetran (limited support)
* Segment (limited support)

## What SaaS Sources are Supported?

* Hubspot CRM (Stitch, Fivetran)
* Harvest Timesheets (Stitch)
* Xero Accounting (Stitch)
* Stripe Payments (Stitch)
* Asana Projects (Stitch)
* Jira Projects (Stitch)
* Mailchimp Email Marketing (Stitch)
* Segment Events (Segment)
* GCP Billing Exports
* Google Ads (Stitch)
* Facebook Ads (Stitch)
* Intercom Messaging (Stitch)
* Mixpanel Events (Stitch, Fivetran)
* Custom data sources

## What Warehouse modules are Modelled?

* Finance (Invoices, Chart of Accounts, Currencies)
* CRM (Deals, Contacts, Companies)
* Projects (Timesheet Projects, Timesheet Tasks, Delivery Projects, Delivery Tasks, Timesheets, Users)
* Marketing (Email lists, Email sends, Email campaigns, Ad Campaigns, Ad Performance, Web Page Views, Web Sessions)

## How Do We Setup a New Environment for Testing, Client Deployment?

See [Setting up a new Warehouse Environment](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/setup.md) for instructions on how to set-up your own dev environment, or a new client environment
