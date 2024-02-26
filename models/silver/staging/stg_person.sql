with
    source_data as (
        select *
        from {{ source('sap_adw', 'person') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["businessentityid"]) }} as sk_person,
            businessentityid as personid,
            persontype,
            namestyle,
            title,
            firstname,
            middlename,
            lastname,
            suffix,
            emailpromotion,
            additionalcontactinfo,
            demographics
        from source_data
    )

select * 
from sk_key