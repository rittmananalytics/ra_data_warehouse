# Rittman Analytics Data Warehouse

## Introduction
This dbt package contains a set of pre-built, pre-integrated Load and Transform dbt models for common SaaS applications.
Objectives for this package are as follows:

1. To standardise how we source and model SaaS data sources in-general
2. To make it simpler to run data quality tests than to not, by defining these tests in-advance
3. To enable merging of customer, product, contact and other shared entity data with no single authoratitive source
4. To pre-create derived analytics measures for individual and combinations of sources
5. In-time, to create a means of selecting sources or subject areas ("modules") and have just those sources/modules loaded (and deployed for a customer)
6. To do all of this in a way that embraces, rather than avoids, community additions to these sources and derived analytics models

## What Databases Are Supported

Right now, just Google BigQuery. Support for Snowflake is the next project priority.

## What SaaS Sources are Currently Supported?
Hubspot CRM
Harvest Timesheets
Xero Accounting
Stripe Payments
Asana Projects
Jira Projects
Mailchimp Email Marketing
Segment Events
GCP Billing Exports
Google Ads
Facebook Ads

## What Warehouse modules are Currently Modelled?

Finance (Invoices, Chart of Accounts, Currencies)
CRM (Deals, Contacts, Companies)
Projects (Timesheet Projects, Timesheet Tasks, Delivery Projects, Delivery Tasks, Timesheets, Users)
Marketing (email lists, email sends, email

## Setting up a dev environment

See [Setting up a New Warehouse Environment](https://github.com/rittmananalytics/ra_data_warehouse/wiki/Setting-up-a-New-Warehouse-Environment).

## Documentation Wiki

Full documentation, design patterns and coding standards are being put together in an [associated Github wiki](https://github.com/rittmananalytics/ra_data_warehouse/wiki).
