# dbt_fivetran_utils v0.2.10
## Bug Fixes
- Added a `dbt_utils.type_string()` cast to the `source_relation` macro. There were accounts of failures occurring within Redshift where the casting was failing in downstream models. This will remedy those issues by casting on field creation if multiple schemas/databases are not provided. ([#53](https://github.com/fivetran/dbt_fivetran_utils/pull/53))

# dbt_fivetran_utils v0.2.9

## Bug Fixes
- Added a specific Snowflake macro designation for the `json_extract_path` macro. ([#50](https://github.com/fivetran/dbt_fivetran_utils/pull/50))
    - This Snowflake version of the macro includes a `try_parse_json` function within the `json_extract_path` function. This allows for the macro to succeed if not all fields are a json object that are being passed through. If a field is not a json object, then a `null` record is generated. 
- Updated the Redshift macro designation for the `json_extract_path` macro. ([#50](https://github.com/fivetran/dbt_fivetran_utils/pull/50))
    - Similar to the above, Redshift cannot parse the field if every record is not a json object. This update converts a non-json field to `null` so the function does not fail.

## Under the Hood
- Included a `union_schema_variable` and a `union_database_variable` which will allow the `source_relation` and `union_data` macros to be used with varying variable names. ([#49](https://github.com/fivetran/dbt_fivetran_utils/pull/49))
    - This allows for dbt projects that are utilizing more than one dbt package with the union source feature to have different variable names and not see duplicate errors.
    - This change needs to be applied at the package level to account for the variable name change. If this is not set, the macros looks for either `union_schemas` or `union_databases` variables.

# dbt_fivetran_utils v0.2.8

## Features
- Added this changelog to capture iterations of the package!
- Added the `add_dbt_source_relation()` macro, which passes the `dbt_source_relation` column created by `union_data()` to `source_relations()` in package staging models. See the README for more details on its appropriate usage.
