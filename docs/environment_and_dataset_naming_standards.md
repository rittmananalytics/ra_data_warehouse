Some input and context over naming of datasets, use of GCP projects and standards around how we use these for dbt projects.

For RA team members, the Rittman Analytics Analytics Workflow & Release Checklists page on Notion has been updated to include these naming standards.

# Historical Naming Approach for Dev and Prod dbt Datasets 

Historically, RA analytics engineers followed these standards when developing dbt projects:

Production. development and test BQ datasets were all in the same GCP project

The production dataset was named ```analytics``` and the only user that could write to it was the dbt GCP service account, via dbtCloud (e.g.  dbt-5-74@ra-development.iam.gserviceaccount.com)

Development took place in a shared dev dataset called ```analytics_dev``` that all developers/users could write to

Test/Staging was done through dbtCloud’s CI/CD test pipeline, that created temporary (as in, deleted automatically by dbtCloud) BQ datasets named in the format ```sinter_pr_xxxx_xx``` with ```xxxx_xx``` reflecting the pull request (PR) number that triggered the test pipeline build.

Note that these test/staging CI/CD temporary datasets are always created in the same GCP project that dbtCloud deploys the analytics dataset to, so consider this as pre-production final deployment testing.

# Dataset Names used by the RA DW dbt Framework 

Our dbt DW framework expanded the number of datasets used for an environment from one (“analytics”) to four, to separate out database objects used for data transformation and process logging from the tables end-users were going to query:

- ```analytics```, the dataset used by end-users and Looker - think of this as the “base” dataset for their dbt environment

- ```analytics_staging```, a dataset containing SQL views and tables used in the data transformation process

- ```analytics_seed```, a dataset containing tables of reference and lookup data populated from files within the dbt project

- ```analytics_logs```, a dataset that contains audit, profile and logging tables created during data loads

Note that all of these datasets are automatically created in BigQuery on first run of the dbt DW framework, as long as the GCP service account used by the developer or dbtCloud has the BigQuery Admin role granted.

For an individual dbt developer, their “base” dataset is determined by the dataset configuration setting in their profiles.yml file

```ra_data_warehouse:
  outputs:
    dev:
      type: bigquery
      method: service-account-json
      project: ra-development
      dataset: analytics_dev
      ```


## Development Environment(s) Naming

As a developer (as opposed to being dbtCloud), our team should use analytics_dev as their dataset value in the profiles.yml configuration file, which would lead to the following dataset names being created on first run of their dbt project:

analytics_dev

analytics_staging_dev

analytics_seed_dev

analytics_logs_dev

This, combined with developing in git feature branches, works out fine for single-developer projects. 

However if there are multiple developers working on the project (note - not doing training exercises), dbt can be configured to use a schema prefix (e.g. “mark”, “lewis”, the developer’s first name) by setting an environment variable before running any of the dbt CLI tools, see the doc notes on how this works.

export schema_prefix=mark

This will thereafter instruct dbt to create dataset names, for individual developer environments, named like this:

mark_analytics_dev

mark_analytics_staging_dev

mark_analytics_seed_dev

mark_analytics_logs_dev

This approach of having a separate dev environment for each developer aligns with the development environment naming approach used by the dbtCloud IDE.


Training Environment(s) Naming
For training environments (defined as dbt development not directly related to the development or maintenance of project feature branches), the developer should create an additional project (environment) definition in their profiles.yml configuration file, separate to the analytics_dev project definition, and use analytics_trn as the base dataset name. Combined with setting a schema_prefix environment variable set to their first name, this should lead to datasets being created like this for training purposes:

mark_analytics_trn

mark_analytics_staging_trn

mark_analytics_seed_trn

mark_analytics_logs_trn


Separation of Production, Development and Training Datasets
Optionally, these production, pre-prod testing/staging, dev and training datasets can be created in their own GCP projects, e.g.

GCP Project

Datasets

 

Production 
and pre-production CI/CD deployment testing

analytics, analytics_staging, analytics_seed, analytics_logs

sinter_pr_xxxx_xx

 

Development

analytics_dev, analytics_staging_dev, analytics_seed_dev, analytics_logs_dev

or for multi-developer projects

firstname_analytics_dev, firstname_analytics_staging_dev, firstname_analytics_seed_dev, firstname_analytics_logs_dev

 

Training

firstname_analytics_trn, firstname_analytics_staging_trn, firstname_analytics_seed_trn, firstname_analytics_logs_trn

 

 
