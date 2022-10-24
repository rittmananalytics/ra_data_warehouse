[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=0.20.x&color=orange)
# Shopify (Source)

This package models Shopify data from [Fivetran's connector](https://fivetran.com/docs/applications/shopify). It uses data in the format described by [this ERD](https://fivetran.com/docs/applications/shopify#schemainformation).

This package enriches your Fivetran data by doing the following:

* Adds descriptions to tables and columns that are synced using Fivetran
* Adds column-level testing where applicable. For example, all primary keys are tested for uniqueness and non-null values.
* Models staging tables, which will be used in our transform package

## Models

This package contains staging models, designed to work simultaneously with our [Shopify modeling package](https://github.com/fivetran/dbt_shopify). The staging models name columns consistently across all packages:
* Boolean fields are prefixed with `is_` or `has_`
* Timestamps are appended with `_at`
* ID primary keys are prefixed with the name of the table. For example, the campaign table's ID column is renamed `campaign_id`.

## Installation Instructions
Check [dbt Hub](https://hub.getdbt.com/) for the latest installation instructions, or [read the dbt docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

Include in your `packages.yml`

```yaml
packages:
  - package: fivetran/shopify_source
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

### Union Multiple Shopify Connectors
If you have multiple Shopify connectors in Fivetran and would like to use this package on all of them simultaneously, we have provided functionality to do so. The package will union all of the data together and pass the unioned table into the transformations. You will be able to see which source it came from in the `source_relation` column of each model. To use this functionality, you will need to set either the `union_schemas` or `union_databases` variables:

```yml
# dbt_project.yml

...
config-version: 2

vars:
    union_schemas: ['shopify_usa','shopify_canada'] # use this if the data is in different schemas/datasets of the same database/project
    union_databases: ['shopify_usa','shopify_canada'] # use this if the data is in different databases/projects but uses the same schema name
```

### Add Passthrough Columns
This package includes all source columns defined in the staging_columns.sql macro. To add additional columns to this package, do so using our pass-through column variables. This is extremely useful if you'd like to include custom fields to the package.

```yml
# dbt_project.yml

...
config-version: 2

vars:
  shopify_source:
    customer_pass_through_columns: []
    order_line_refund_pass_through_columns: []
    order_line_pass_through_columns: []
    order_pass_through_columns: []
    product_pass_through_columns: []
    product_variant_pass_through_columns: []
```

### Disable Models
This package was designed with the intention that users have all relevant Shopify tables being synced by Fivetran. However, if you are a Shopify user that does not operate on returns or adjustments then you will not have the related source tables. As such, you may use the below variable configurations to disable the respective downstream models. All variables are `true` by default. Only add the below configuration if you are wishing to disable the models:

```yml
# dbt_project.yml

...
vars:
  shopify__using_order_adjustment:  false  # true by default
  shopify__using_order_line_refund: false  # true by default
  shopify__using_refund:      false  # true by default
```

### Changing the Build Schema
By default this package will build the Shopify staging models within a schema titled (<target_schema> + `_stg_shopify`) in your target database. If this is not where you would like your staging Shopify data to be written to, add the following configuration to your `dbt_project.yml` file:

```yml
# dbt_project.yml

...
models:
  shopify_source:
    +schema: my_new_schema_name # leave blank for just the target_schema
```

## Contributions

Additional contributions to this package are very welcome! Please create issues
or open PRs against `main`. Check out 
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
