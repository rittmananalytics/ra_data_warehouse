## Introduction

This repository contains a set of pre-built dbt Load and Transform models for common SaaS applications to create an integrated ("conformed") data warehouse dimensional model. In its current incarnation it supports Google BigQuery as the target data warehouse and Stitch (and Segment warehouse destinations) as the data pipeline technology.

### Current Dimensional Model

![Dimensional Model](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/ra_dw/img/dimensional_model.png)

## Design Goals

1. To standardise how we source and model SaaS data sources in-general
2. To make it simpler to run data quality tests than to not, by defining these tests in-advance
3. To enable merging of customer, product, contact and other shared entity data with no single authoratitive source
4. To pre-create derived analytics measures for individual and combinations of sources
5. To create a means of selecting sources or subject areas ("modules") and have just those sources/modules loaded (and deployed for a customer)
6. To enable use of either Stitch, Fivetran or Segment as the pipeline technology based on client need
7. To enable loading and integration of custom (customer app database) sources into the warehouse

## What are the Layers in the Warehouse Data Model?

![Model Layers](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/ra_dw/img/data_flow.png)

See [Design and Coding Approach](https://github.com/rittmananalytics/ra_data_warehouse/wiki/Design-and-Coding-Approach) for Implementation Details

## What SaaS Sources are Currently Supported?

* Hubspot CRM
* Harvest Timesheets
* Xero Accounting
* Stripe Payments 
* Asana Projects
* Jira Projects
* Mailchimp Email Marketing
* Segment Events 
* GCP Billing Exports 
* Google Ads 
* Facebook Ads
* Intercom Messaging
* Mixpanel Events
* Custom data sources

data sources on roadmap for Q2 2020 include:

* Salesforce CRM
* Bamboo HRMS
* Chargebee Payments

* Google Analytics (Free & 360)

## What Warehouse modules are Currently Modelled?

* Finance (Invoices, Chart of Accounts, Currencies)
* CRM (Deals, Contacts, Companies)
* Projects (Timesheet Projects, Timesheet Tasks, Delivery Projects, Delivery Tasks, Timesheets, Users)
* Marketing (Email lists, Email sends, Email campaigns, Ad Campaigns, Ad Performance, Web Page Views, Web Sessions)

## How Do We Setup a New Environment for Testing, Client Deployment?

See [Setting up a new Warehouse Environment](https://github.com/rittmananalytics/ra_data_warehouse/wiki/Setting-up-a-New-Warehouse-Environment) for instructions on how to set-up your own dev environment, or a new client environment
