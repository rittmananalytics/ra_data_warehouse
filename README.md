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

## Setting up a dev environment

From the Terminal CLI, set the `schema_prefix` env variable:

```
export schema_prefix=test_dev
```

Now run the following `dbt run operation` command supplying values for these parameters:

- staging : staging dataset suffix, typically `staging`
- seed : seed dataset suffix, typically `seed`
- logs : logs dataset suffix, typically `logs`

```
dbt run-operation bootstrap_schemas --args '{staging_schema_name: staging, seed_schema_name: seed,  logs_schema_name: logs, reset : true}' --profile ra_data_warehouse --target ra_dw_dev
```

To drop all schemas, first set the `schema_prefix` env variable:

```
export schema_prefix=test_dev
```

Then run this `dbt run operation` command:

```
dbt run-operation drop_schemas --args '{staging_schema_name: staging, seed_schema_name: seed,  logs_schema_name: logs}' --profile ra_data_warehouse --target ra_dw_dev
```

## Documentation Wiki

Full documentation, design patterns and coding standards are being put together in an [associated Github wiki](https://github.com/rittmananalytics/ra_data_warehouse/wiki).
