## Setting Up a New Warehouse Environment

1. Clone the Warehouse Git Repo

Using Github Desktop or the git CLI on Terminal, clone the git repo:

```
git clone git@github.com:rittmananalytics/ra_data_warehouse.git
```

### Configuring Data Sources

2. Create or edit `profiles.yml` with the following content and place it under `~/.dbt/` on your machine. For safety, `dev` is a default target.

```yaml
ra_data_warehouse:
  outputs:
    dev:
      type: bigquery
      method: service-account-json
      project: [your GCP project id]
      dataset: analytics_dev
      location: [your GCP data location]
      threads: 1
      timeout_seconds: 300
      keyfile_json:
        type: service_account
        project_id: [your GCP project id]
        private_key_id: [your private key id]
        private_key: [your private key, with quotes (") aroumd key]
        client_email: [your client_email]
        client_id: [your client ID]
        auth_uri: https://accounts.google.com/o/oauth2/auth
        token_uri: https://oauth2.googleapis.com/token
        auth_provider_x509_cert_url: https://www.googleapis.com/oauth2/v1/certs
        client_x509_cert_url: [your GCP pclient_x509_cert_url]
    prd:
      type: bigquery
      method: service-account-json
      project: [your GCP project id]
      dataset: analytics
      location: [your GCP data location]
      threads: 1
      timeout_seconds: 300
      keyfile_json:
        type: service_account
        project_id: [your GCP project id]
        private_key_id: [your private key id]
        private_key: [your private key, with quotes (") aroumd key]
        client_email: [your client_email]
        client_id: [your client ID]
        auth_uri: https://accounts.google.com/o/oauth2/auth
        token_uri: https://oauth2.googleapis.com/token
        auth_provider_x509_cert_url: https://www.googleapis.com/oauth2/v1/certs
        client_x509_cert_url: [your GCP pclient_x509_cert_url]
```

4. Enable or Disable Data Sources and Warehouse Modules

Using a text editor, edit the `dbt_project.yml` config file to enable/disable individual data sources or warehouse modules

```yaml
vars:
      enable_harvest_projects_source:      [true|false]
      enable_hubspot_crm_source:           [true|false]
      enable_asana_projects_source:        [true|false]
      enable_jira_projects_source:         [true|false]
      enable_stripe_payments_source:       [true|false]
      enable_xero_accounting_source:       [true|false]
      enable_mailchimp_email_source:       [true|false]
      enable_segment_events_source:        [true|false]
      enable_google_ads_source:            [true|false]
      enable_facebook_ads_source:          [true|false]
      enable_intercom_messaging_source:    [true|false]
      enable_custom_source_1:              [true|false]
      enable_custom_source_2:              [true|false]
      enable_mixpanel_events_source:       [true|false]
# warehouse modules
      enable_crm_warehouse:         [true|false]e
      enable_finance_warehouse:     [true|false]
      enable_projects_warehouse:    [true|false]
      enable_marketing_warehouse:   [true|false]
      enable_ads_warehouse:         [true|false]
      enable_product_warehouse:     [true|false]
```

5. Then, within the same `dbt_project.yml` config file and for each data source enabled, provide the schema and table name for each table within the data source and if multiple ETL pipeline options are available, the pipeline technology.

For example, for Facebook Ads where only Stitch is supported as the ETL pipeline

```yaml
stg_facebook_ads:
              vars:
                  id-prefix: [Your datasource prefix, unique for each source, e.g. fbads-]
                  stitch_adcreative_table: [Your Stitch dataset and table name, e.g. stitch_facebook_ads.adcreative]
                  stitch_ads_table: [Your Stitch dataset and table name, e.g. stitch_facebook_ads.ads]
                  stitch_adsets_table: [Your Stitch dataset and table name, e.g. stitch_facebook_ads.adsets]
                  stitch_campaigns_table: [Your Stitch dataset and table name, e.g. stitch_facebook_ads.campaigns]
                  stitch_ads_insights_age_and_gender_table: [Your Stitch dataset and table name, e.g. stitch_facebook_ads.ads_insights_age_and_gender]
                  stitch_ads_insights_table: [Your Stitch dataset and table name, e.g. stitch_facebook_ads.ads_insights]
```

For Mixpanel where both Stitch and Fivetran are supported as ETL pipelines, provide a value for the `etl` variable to indicate whether Stitch or Fivetran is the pipeline technology for this data source (note that you can use Stitch for some data sources and Fivetran for others, and you can create copies of data source adapters if you have one source using one and one using the other as long as the id-prefix value is unique for each data source)

```yaml
stg_mixpanel_events:
              vars:
                  id-prefix: [Your datasource prefix, unique for each source, e.g. mixpanel-]
                  etl: [fivetran|stitch]
                  fivetran_event_table: [Your Fivetran dataset and table name,fivetran_mixpanel.event]
                  stitch_export_table: [Your Stitch dataset and table name, e.g. mixpanel_stitch.export]

```

Note also that some data sources have variables specific to just those data sources, for example `staff_email_domain:` for the stg_asana_projects data source; also note the variables set with default values in the `integration:` section.

### Configuring BigQuery Table and Dataset Settings

6. You can also configure the dataset prefixes used for the sources, integration, utilities and warehouse database objects as well as whether they are created as SQL views, tables or ephemeral (sub-queries) in the `dbt_project.yml`, or leave them at their default values.

```yaml
models:
  ra_data_warehouse:
# data source general settings
      sources:
          materialized: [view|ephemeral|table, default is `view`]
          schema: [schema prefix for source models, default is `staging`]
# integration layer settings
      integration:
          materialized: [view|ephemeral|table, default is view]
          schema: staging [schema prefix for integration models, default is `staging`]
          vars:
              web_sessionization_trailing_window: 3
              web_inactivity_cutoff: 30 * 60
# warehouse layer settings
      warehouse:
          materialized: [view|ephemeral|table, default is table]
# util module settings
      utils:
          materialized: [view|ephemeral|table, default is view]
          schema: [schema prefix for utils models, default is `logs`]
# seed module settings
seeds:
  ra_data_warehouse:
    schema: [schema prefix for utils models, default is `seeds`]
```

### Other Setup Steps

7. Configure and connect your Stitch, Fivetran, Segment and other data sources in-line with the configuration settings you have provided in steps 4,5 and 6 so that raw data from each of your SaaS data sources is now available within your data warehouse, ready for transformation.

8. [Other information on setting up machine](https://discourse.getdbt.com/t/how-we-set-up-our-computers-for-working-on-dbt-projects/243)

### CLI Steps

9. In the command line, with current directory set to be in the root of the project (`~/dbt`), optionally set a schema prefix to be used in-front of each of the dataset names dbt will be creating for you, for example to create multiple RA Warehouse environments within the same GCP project.

```
export schema_prefix=mark
```

Then run [dbt deps](https://docs.getdbt.com/docs/deps) to collect latest versions of dependencies, open-source dbt packages etc.

```
dbt deps
```

Then import your seed data files:
```
dbt seed --profile ra_data_warehouse --target dev
```

10. Finally, start your first data warehouse load.
```
dbt run --profile ra_data_warehouse --target dev
```
