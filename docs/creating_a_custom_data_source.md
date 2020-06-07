
1. Open the dbt_project.yml file and locate the enable_custom_source_1 variable.

```
vars:
      ...
      enable_custom_source_1:              false
      enable_custom_source_2:              false
      ...
```
Change it's name to reflect the source you'll be adding, e.g. `enable_product_database_source` and set its value to `true`, like this:
```
vars:
      ...
      enable_bidlogix_auctions_source:     false
      enable_custom_source_2:              false
      ...
```
If this source is likely to require an additional warehouse module, add it to the `#warehouse modules` section underneath these data source settings, e.g.

```
# warehouse modules
      enable_crm_warehouse:         true
      ...
      enable_auctions_warehouse:    true
```

2. Now, also within the dbt_project.yml file, add a new [model configuration](https://docs.getdbt.com/reference/model-configs) under the existing pre-packaged data source model configurations to provide values for the `id-prefix`,`etl`,`stitch_schema` and/or `fivetran_schema` standard model variables as well as variables for each of the source tables your data source adapter will import.

- `id-prefix` is a alphanumeric prefix to be added to every ID column used in your source adapter, to ensure these ID values are globally unique when we later on merge this source with data from other data sources.
- `etl` is for when you want the ETL pipeline technology to be selectable between Stitch, Fivetran or Segment; set the value to either `stitch`, `fivetran` or `segment` and note that (1) only one pipeline technology can be used per source adapter and (2) it is down to you as the developer to implement the pipeline-specific data transformations for your adapter
- `stitch_schema`, `fivetran_schema` and `segment_schema` are used for specifying the dataset that contains your incoming raw dataset. If you're only implementing Stitch as the pipeline technology then you only need to provide values for `stitch_schema`, same for Fivetran (or Segment)
- `stitch_<table_name>_table` and the equivalent `fivetran_` and `segment_` variants are used to specify the actual table names used for each of your data source raw incoming tables. All of these variables are then used in the data source adapter you'll define in the next step.

An example model configuration for a custom data source used for auction data is shown below:

```yaml
stg_bidlogix_auctions:
              vars:
                  id-prefix: bidlogix-
                  etl: stitch
                  stitch_schema: bidlogix_mysql_slave_lewes
                  stitch_bids_table: aa_bid
                  stitch_auctions_table: aa_auction
                  stitch_categories_table: aa_category
                  stitch_content_table: aa_content
                  stitch_listings_table: aa_listing
                  stitch_invoices_table: inv_invoice
                  stitch_invoice_versions_table: inv_invoice_version
                  stitch_invoice_details_table: inv_invoice_details
                  stitch_invoice_details_version_table: inv_invoice_details_version
                  stitch_line_items_table: inv_line_item
                  stitch_line_item_versions_table: inv_line_item_version
                  stitch_link_invoice_ver_line_item_ver_table: inv_link_invoice_ver_line_item_ver
                  stitch_currencies_table: ref_currency
                  stitch_users_table: usr_user
```

3. Then, within the repo under the /models/sources directory, locate the stg_custom_source_1 subdirectory. This directory and the model files within it should have the following structure:

```
stg_custom_source_1
├── schema
│   └── schema.yml
├── stg_custom_1_accounts.sql
├── stg_custom_1_companies.sql
├── stg_custom_1_contacts.sql
├── stg_custom_1_products.sql
├── stg_custom_1_projects.sql
├── stg_custom_1_transactions.sql
└── stg_custom_1_users.sql
```
Rename the directory to match the name you gave your adapter in the `dbt_project.yml` file, for example `stg_bidlogic_auctions`.

Then, for each of the model (.sql) files within this directory that will be relevant to your data source, rename those files to replace the `custom_1` element to instead use your data source name, for example `stg_<your_source_name>_contacts.sql`. Finally delete any model files that won't be implemented by your source adapter, so that your final list of files looks similar to the one below (your particular selection of implemented models may of course differ from the ones listed below)
```
├── schema
│   ├── schema.yml
├── stg_<your_source_name>_companies.sql
├── stg_<your_source_name>_contacts.sql
├── stg_<your_source_name>_currencies.sql
└── stg_<your_source_name>_invoices.sql
```

4. With each of the model files, locate the part of the model definition that disables the model if the data source is not set to `true` in the dbt_project.yml file.

```
{% if not var("enable_custom_source_1") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
```

Replace `enable_custom_source_1` with the data source enablement variable you defined in step 1 (in this example, `enable_bidlogix_auctions_source`) so that it now looks like this, using your own source enable variable name:

```{% if not var("enable_bidlogix_auctions_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
```

Then within the source definition which by default looks like this:

```
WITH source AS (
    select *
    from
    {{ source('custom_source_1','s_accounts' ) }}
),
```

replace the SELECT query with the following dbt macro call, using the `stitch_schema` and relevant `stitch_` table name for the particular model file:

```
{{ filter_stitch_table(var('stitch_schema'),var('stitch_clients_table'),'id') }}
```

Note that `stitch_schema` is a model-scoped dbt variable that can have different values for each of the models (source adapters) in your project.

Then, within the `renamed` section, locate the `concat('custom_1-',id)` expression and replace it with `concat('{{ var('id-prefix') }}',id)` so that it uses your source adapter prefix defined using the `id-prefix` variable you set in the dbt_projects.yml file. Finally, replace the placeholder expressions for each or some of the columns in the rest of this sections' query definition which by default look like this:

```
renamed as (
  SELECT
  concat('{{ var('id-prefix') }}',id) AS contact_id,
  cast(null as string) AS contact_first_name,
  cast(null as string) AS contact_last_name,
  cast(null as string) AS contact_name,
  cast(null as string) AS contact_job_title,
  cast(null as string) AS contact_email,
  cast(null as string) AS contact_phone,
  cast(null as string) AS AS contact_phone_mobile,
  ccast(null as string) AS contact_address,
  cast(null as string) AS contact_city,
  ccast(null as string) AS contact_state,
  cast(null as string) AS contact_country,
  ccast(null as string) AS contact_postcode_zip,
  cast(null as string) AS contact_company,
  ccast(null as string) AS contact_website,
  cast(null as string) AS AS contact_company_id,
  cast(null as string) AS contact_owner_id,
  cast(null as string) AS contact_lifecycle_stage,
  cast(null as timestamp) AS contact_created_date,
  cast(null as timestamp) AS contact_last_modified_date
FROM
  source
)
```

An example customized query definition is shown below:

```
renamed as (
  SELECT
  concat('{{ var('id-prefix') }}',id) AS contact_id,
  concat(upper(substr(lower(forename),1,1)),lower(substr(forename,2))) AS contact_first_name,
  concat(upper(substr(lower(surname),1,1)),lower(substr(surname,2))) AS contact_last_name,
  concat(concat(upper(substr(lower(forename),1,1)),lower(substr(forename,2))),' ',concat(upper(substr(lower(surname),1,1)),lower(substr(surname,2)))) AS contact_name,
  job_title AS contact_job_title,
  email AS contact_email,
  phone_number AS contact_phone,
  mobile_number AS contact_mobile_phone,
  cast(null as string) AS contact_address,
  cast(null as string) AS contact_city,
  cast(null as string) AS contact_state,
  cast(null as string) AS contact_country,
  cast(null as string) AS contact_postcode_zip,
  company_name AS contact_company,
  split(email,'@')[safe_offset(1)] AS contact_website,
  cast(null as string) AS contact_company_id,
  --cast(null as string) AS contact_owner_id,
  --cast(null as string) AS contact_lifecycle_stage,
  billing_address_id as contact_billing_address_id,
  shipping_address_id as contact_shipping_address_id,
  inserted AS contact_created_date,
  updated AS contact_last_modified_date
FROM
  source
)
```

Note that you do not need to implement all of the templated columns, at a minumum you should always implement the `id` column but anything else can be left blank with the exception of `contact_name` for the contacts table. 
