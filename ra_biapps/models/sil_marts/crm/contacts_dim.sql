{{
    config(
        materialized='table',
        unique_key='contact_pk'
    )
}}
SELECT
   source,
   concat(source,contact_id) as contact_pk,
   contact_id,
   contact_first_name,
   contact_last_name,
   contact_name,
   contact_job_title,
   contact_email,
   contact_phone,
   contact_mobile_phone,
   contact_address,
   contact_city,
   contact_state,
   contact_postcode_zip,
   contact_company,
   contact_website,
   contact_company_id,
   contact_owner_id,
   contact_lifecycle_stage,
   contact_created_date,
   contact_last_modified_date
FROM
   {{ ref('sde_hubspot_crm_contacts_ds') }}

{% if is_incremental() %}

     -- this filter will only be applied on an incremental run
     where contact_created_date > (select max(contact_created_date) from {{ this }})
        or contact_last_modified_date > (select max(contact_last_modified_date) from {{ this }})

   {% endif %}
