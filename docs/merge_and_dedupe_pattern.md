## Dimension Merge and Deduplication Across Multiple Data Sources

Customers, contacts, projects and other shared dimensions are automatically created from all data sources, deduplicating by name and merge lookup files using a process that preserves source system keys whilst assigning a unique ID for each customer, contact etc.

### Design Pattern

1. Each sta_* source adapter t_* source view for the dimension provides a unique ID, prefixed with the source name, and another field value (for example, user name) that can be used for deduplicating dimension members downstream. These fields are then initially merged (UNION ALL) together in the i_* integration view.

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/sta_dimension_sources_to_int_merge.png)

2. An CTE containing an array of source dimension IDs is then created within the i_ integration view, grouped by the deduplication column (in this example, user name)

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/create_array_of_source_ids_with_dedupe_column.png)

Any other multivalue columns are similarly-grouped by the deduplication column in further CTEs within the i_ integration view, for example list of email addresses for a user.

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/array_of_other_multivalue_columns_with_dedupe_column.png)

For dimensions where merging of members by name is not sufficient (for example, company names that cannot be relied on to always be spelt the same across all sources) we can add seed files to map one member to another and then extend the logic of the merge to make use of this merge file, for example:

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/complex_merge_dedupe_with_merge_list.png)

3. Within the i_ integration view, all remaining columns are then deduplicated by the deduplication column.

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/dedupe_integration_view.png)

4. Then this deduplicated CTE is joined-back to the CTE, along with any other multivalue column CTEs

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/join_back_id_and_other_multivalue_arrays.png)

5. The wh_ warehouse dimension table then adds a unique GUID for each dimension member as a surrogate key.

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/load_warehouse_dimension.png)

6. The i_ integration view for the associated fact table contains rows referencing these deduplicated dimension members using the source system IDs e.g. 'harvest-2122', 'asana-22122'

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/fact_source_integration.png)

7. When loading the associated wh_ fact table, the lookup to the wh_ dimension table uses UNNEST() to query the array of source system IDs, returning the wh_ dimension GUID as the dimension surrogate key

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/load_warehouse_fact_using_multivalue_source_id_arrays.png)

8. The wh_ dimension table contains the source system IDs and other multivalue dimension columns as BigQuery repeating columns

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/dimension_table_with_multivalue_source_ids_and_other_columns.png)

9. The wh_ fact table contains dimension foreign keys in the form of these GUIDs, as opposed to the source IDs for the dimensions being joined to.

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/fact_table_with_dimension_guids.png)
