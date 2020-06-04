## Company and Contact Record Enrichment

A typical way to set-up enrichment is to use a service such as [Clearbit Enrichment](https://clearbit.com/enrichment) in-combination with a service such as [Zapier](https://zapier.com/apps/clearbit/integrations).

For example, a Zapier Zap listens for new contact records being created in HubSpot, and uses details of those new contacts to call Clearbit's Enrichment REST API using the new contact's email address. The results of the API call are then sent to Google BigQuery as a new row in a pre-defined landing table, and that table is the used as the input into the RA Warehouse load process.

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/enrichment.png)


### Design Pattern

To enable enrichment, set the following settings to "true" in the dbt_project.yml config file. Note that contact records, company records or both types of record can be enabled for enrichment.

```yaml
      enable_clearbit_enrichment_source:   true
      contacts_enrichment: true
      companies_enrichment: true
```

Then within the source adapters section of the dbt_projects.yml file, provide the id-prefix, schema and table names for the table(s) that contain your enrichment data.

```yaml
stg_clearbit_enrichment:
              vars:
                  id-prefix: clearbit-
                  clearbit_schema: enrichment
                  clearbit_contacts_table: contacts_companies
                  clearbit_companies_table: contacts_companies
                  tags: ["clearbit", "crm","enrichment"]
```

When enrichment happens, existing contact records are matched to contacts in the enrichment tables using the contact's email. For companies, matching takes place using the domain name (e.g. getdbt.com) for the company.

Within the source adapter section of the project, there is an adapter for Clearbit Enrichment. You will need to amend the standard set of incoming table columns to match the particular columns you returned from the Clearbit Enrichment API. Note that all the enrichment columns you define for a contact or company will be added onto the end of the standard contact and company columns, for all data sources, where there is a match on email address (contacts) or web domain (companies).

```yaml
── stg_clearbit_enrichment
   ├── stg_clearbit_enrichment_companies.sql
   └── stg_clearbit_enrichment_contacts.sql
```

![](https://github.com/rittmananalytics/ra_data_warehouse/blob/master/img/enrich.png)

When you enable contact or company record enrichment the stg_clearbit_enrichment source adapter becomes enabled and dbt creates two views, one for companies and one for contacts, containing your enrichment data. Then these two views are joined to the normal results of merging together companies and contacts in the int_companies and int_contacts views (only when this data source, and either company or contact enrichment is enabled) through conditional logic such as this example from the int_contacts view.

```
{% if var("enable_clearbit_enrichment_source") and var("companies_enrichment") %}
,
enriched_companies as (
  select *
  from {{ ref ('stg_clearbit_enrichment_companies') }}
),
joined as (
      SELECT c.*,
             e.*
      FROM merged c
      LEFT OUTER JOIN enriched_companies e
      ON c.company_website = e.company_enrichment_website_domain
)
select * from joined

{% else %}

select * from merged

{% endif %}
```

Results of this enrichment are then used to populate the downstream companies_dim warehouse table. If company enrichment is disabled them just the regular set of (merged) company records are returned by this view and used to populate the companies_dim table.
