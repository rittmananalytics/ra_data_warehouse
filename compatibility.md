# Compatibility Matrix

## Google BigQuery (Standard SQL)

|Data Source                                        |HubSpot CRM     |Harvest Projects|Jira Projects|Asana Projects|Xero Accounting|Stripe Payments|Mailchimp Email|Hubspot Email|Facebook Ads   |Google Ads     |Segment Events|Mixpanel Events |Stripe Subscriptions|Intercom Messaging|
|---------------------------------------------------|----------------|----------------|-------------|--------------|---------------|---------------|---------------|-------------|---------------|---------------|--------------|----------------|--------------------|------------------|
|crm_warehouse_company_sources                      |Fivetran, Stitch|Stitch          |Stitch       |Stitch        |Stitch         |Stitch         |               |             |               |               |              |                |Segment             |Stitch            |
|crm_warehouse_contact_sources                      |Fivetran, Stitch|Stitch          |Stitch       |Stitch        |Stitch         |Stitch         |Stitch         |Stitch       |               |               |              |                |                    |Stitch            |
|crm_warehouse_conversations_sources                |Fivetran, Stitch|                |             |              |               |               |               |             |               |               |              |                |                    |                  |
|marketing_warehouse_ad_campaign_sources            |                |                |             |              |               |               |Stitch         |Stitch       |               |Stitch, Segment|              |                |                    |                  |
|marketing_warehouse_ad_campaign_performance_sources|                |                |             |              |               |               |Stitch         |Stitch       |Stitch, Segment|Stitch, Segment|              |                |                    |                  |
|marketing_warehouse_ad_performance_sources         |                |                |             |              |               |               |               |             |Stitch, Segment|Stitch, Segment|              |                |                    |                  |
|marketing_warehouse_ad_group_sources               |                |                |             |              |               |               |               |             |Stitch, Segment|Stitch, Segment|              |                |                    |                  |
|marketing_warehouse_ad_sources                     |                |                |             |              |               |               |               |             |Stitch, Segment|Stitch, Segment|              |                |                    |                  |
|marketing_warehouse_email_event_sources            |                |                |             |              |               |               |Stitch         |Stitch       |               |               |              |                |                    |                  |
|marketing_warehouse_email_list_sources             |                |                |             |              |               |               |Stitch         |Stitch       |               |               |              |                |                    |                  |
|marketing_warehouse_deal_sources                   |Fivetran, Stitch|                |             |              |               |               |               |             |               |               |              |                |                    |                  |
|projects_warehouse_delivery_sources                |                |                |Stitch       |Stitch        |               |               |               |             |               |               |              |                |                    |                  |
|projects_warehouse_timesheet_sources               |                |Stitch          |             |              |               |               |               |             |               |               |              |                |                    |                  |
|finance_warehouse_invoice_sources                  |                |Stitch          |             |              |Stitch         |Stitch         |               |             |               |               |              |                |                    |                  |
|finance_warehouse_transaction_sources              |                |                |             |              |Stitch         |Stitch         |               |             |               |               |              |                |                    |                  |
|finance_warehouse_payment_sources                  |                |                |             |              |Stitch         |Stitch         |               |             |               |               |              |                |                    |                  |
|product_warehouse_event_sources                    |                |                |             |              |               |               |               |             |               |               |Segment       |Fivetran, Stitch|                    |                  |
|subscriptions_warehouse_sources                    |                |                |             |              |               |               |               |             |               |               |              |                |Segment             |                  |

## Snowflake Data Warehouse

|Data Source                                        |HubSpot CRM     |Harvest Projects|Jira Projects|Mailchimp Email|Hubspot Email|Facebook Ads    |Google Ads      |Segment Events|
|---------------------------------------------------|----------------|----------------|-------------|---------------|-------------|----------------|----------------|--------------|
|crm_warehouse_company_sources                      |Stitch          |Stitch          |Stitch       |               |             |                |                |              |
|crm_warehouse_contact_sources                      |Stitch          |Stitch          |Stitch       |Stitch         |Stitch       |                |                |              |
|crm_warehouse_conversations_sources                |Stitch          |                |             |               |             |                |                |              |
|marketing_warehouse_ad_campaign_sources            |                |                |             |Stitch         |Stitch       |                |Stitch, Segment |              |
|marketing_warehouse_ad_campaign_performance_sources|                |                |             |Stitch         |Stitch       |Stitch, Segment |Stitch, Segment |              |
|marketing_warehouse_ad_performance_sources         |                |                |             |               |             |Stitch, Segment |Stitch, Segment |              |
|marketing_warehouse_ad_group_sources               |                |                |             |               |             |Stitch, Segment |Stitch, Segment |              |
|marketing_warehouse_ad_sources                     |                |                |             |               |             |Stitch, Segment |Stitch, Segment |              |
|marketing_warehouse_email_event_sources            |                |                |             |Stitch         |Stitch       |                |                |              |
|marketing_warehouse_email_list_sources             |                |                |             |Stitch         |Stitch       |                |                |              |
|marketing_warehouse_deal_sources                   |Stitch          |                |             |               |             |                |                |              |
|projects_warehouse_delivery_sources                |                |                |Stitch       |               |             |                |                |              |
|projects_warehouse_timesheet_sources               |                |Stitch          |             |               |             |                |                |              |
|finance_warehouse_invoice_sources                  |                |Stitch          |             |               |             |                |                |              |
|finance_warehouse_transaction_sources              |                |                |             |               |             |                |                |              |
|finance_warehouse_payment_sources                  |                |                |             |               |             |                |                |              |
|product_warehouse_event_sources                    |                |                |             |               |             |                |                |Segment       |
|subscriptions_warehouse_sources                    |                |                |             |               |             |                |                |              |
