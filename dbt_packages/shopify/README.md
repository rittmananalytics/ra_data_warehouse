[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=0.20.x&color=orange)
# Shopify

This package models Shopify data from [Fivetran's connector](https://fivetran.com/docs/applications/shopify). It uses data in the format described by [this ERD](https://fivetran.com/docs/applications/shopify#schemainformation).

The main focus of the package is to transform the core object tables into analytics-ready models, including a cohort model to understand how your customers are behaving over time.

## Models

This package contains transformation models, designed to work simultaneously with our [Shopify source package](https://github.com/fivetran/dbt_shopify_source). A dependency on the source package is declared in this package's `packages.yml` file, so it will automatically download when you run `dbt deps`. The primary outputs of this package are described below.

| **model**                 | **description**                                                                                                    |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| [shopify__customer_cohorts](models/shopify__customer_cohorts.sql)  | Each record represents the monthly performance of a customer, including fields for the month of their 'cohort'.    |
| [shopify__customers](models/shopify__customers.sql)        | Each record represents a customer, with additional dimensions like lifetime value and number of orders.            |
| [shopify__orders](models/shopify__orders.sql)           | Each record represents an order, with additional dimensions like whether it is a new or repeat purchase.           |
| [shopify__order_lines](models/shopify__order_lines.sql)     | Each record represents an order line item, with additional dimensions like how many items were refunded.           |
| [shopify__products](models/shopify__products.sql)         | Each record represents a product, with additional dimensions like most recent order date and order volume.         |
| [shopify__transactions](models/shopify__transactions.sql)     | Each record represents a transaction with additional calculations to handle exchange rates.                        |


## Installation Instructions
Check [dbt Hub](https://hub.getdbt.com/) for the latest installation instructions, or [read the dbt docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

Include in your `packages.yml`

```yaml
packages:
  - package: fivetran/shopify
    version: [">=0.5.0", "<0.6.0"]
```

## Configuration
By default, this package looks for your Shopify data in the `shopify` schema of your [target database](https://docs.getdbt.com/docs/running-a-dbt-project/using-the-command-line-interface/configure-your-profile). If this is not where your Shopify data is, add the following configuration to your `dbt_project.yml` file:

```yml
# dbt_project.yml

...
config-version: 2

vars:
    shopify_database: your_database_name
    shopify_schema: your_schema_name
```

If you have multiple Shopify connectors in Fivetran and would like to use this package on all of them simultaneously, we have provided functionality to do so. The package will union all of the data together and pass the unioned table into the transformations. You will be able to see which source it came from in the `source_relation` column of each model. To use this functionality, you will need to set either the `union_schemas` or `union_databases` variables:

```yml
# dbt_project.yml

...
config-version: 2

vars:
    union_schemas: ['shopify_usa','shopify_canada'] # use this if the data is in different schemas/datasets of the same database/project
    union_databases: ['shopify_usa','shopify_canada'] # use this if the data is in different databases/projects but uses the same schema name
```

### Changing the Build Schema
By default this package will build the Shopify staging models within a schema titled (<target_schema> + `_stg_shopify`) and the Shopify final models within a schema titled (<target_schema> + `_shopify`) in your target database. If this is not where you would like your modeled Shopify data to be written to, add the following configuration to your `dbt_project.yml` file:

```yml
# dbt_project.yml

...
models:
  shopify:
    +schema: my_new_schema_name # leave blank for just the target_schema
  shopify_source:
    +schema: my_new_schema_name # leave blank for just the target_schema
```

For additional configurations for the source models, visit the [Shopify source package](https://github.com/fivetran/dbt_shopify_source).

## Contributions

Additional contributions to this package are very welcome! Please create issues
or open PRs against `master`. Check out 
[this post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) 
on the best workflow for contributing to a package.

## Database support
This package has been tested on BigQuery, Snowflake, Redshift, Postgres, and Databricks.

### Databricks Dispatch Configuration
dbt `v0.20.0` introduced a new project-level dispatch configuration that enables an "override" setting for all dispatched macros. If you are using a Databricks destination with this package you will need to add the below (or a variation of the below) dispatch configuration within your `dbt_project.yml`. This is required in order for the package to accurately search for macros within the `dbt-labs/spark_utils` then the `dbt-labs/dbt_utils` packages respectively.
```yml
# dbt_project.yml

dispatch:
  - macro_namespace: dbt_utils
    search_order: ['spark_utils', 'dbt_utils']
```

## Resources:
- Provide [feedback](https://www.surveymonkey.com/r/DQ7K7WW) on our existing dbt packages or what you'd like to see next
- Have questions, feedback, or need help? Book a time during our office hours [using Calendly](https://calendly.com/fivetran-solutions-team/fivetran-solutions-team-office-hours) or email us at solutions@fivetran.com
- Find all of Fivetran's pre-built dbt packages in our [dbt hub](https://hub.getdbt.com/fivetran/)
- Learn how to orchestrate [dbt transformations with Fivetran](https://fivetran.com/docs/transformations/dbt)
- Learn more about Fivetran overall [in our docs](https://fivetran.com/docs)
- Check out [Fivetran's blog](https://fivetran.com/blog)
- Learn more about dbt [in the dbt docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the dbt blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
