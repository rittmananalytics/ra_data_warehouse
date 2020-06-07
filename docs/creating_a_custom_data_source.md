
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

3. Then, within the repo under the /models/sources directory, locate the stg_custom_source_1 subdirectory and rename it to match the name you gave your adapter in the dbt_project.yml file. Then, for each of the model (.sql) files within this directory that will be relevant to your data source, rename the files from `stg_custom_source_contacts.sql`, for example, to `stg_<your_source_name>_contacts.sql` and delete any model files that aren't relevant so that your adapter directory looks like this:
```
├── schema
│   ├── schema.yml
├── stg_bidlogix_auctions_companies.sql
├── stg_bidlogix_auctions_contacts.sql
├── stg_bidlogix_auctions_currencies.sql
└── stg_bidlogix_auctions_invoices.sql
```
