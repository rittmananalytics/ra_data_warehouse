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

### Current Modules

| Module       | Implemented Sources | Planned Sources             |
|--------------|---------------------|-----------------------------|
| CRM & Sales  | Hubspot CRM         | Salesforce CRM              |
| Projects     | Harvest Timesheets, Asana, Jira  | Netsuite Projects           |
| Finance      | Xero, Stripe                | Chargebee, Freshbooks                    |
| Marketing    | Mailchimp           | Salesforce Marketing Cloud, Hubspot Email Marketing  |
| Events + Web | Segment             | Mixpanel, Google Analytics (Free), Qubit Live Tap                   |
