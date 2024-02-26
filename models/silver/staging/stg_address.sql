with
    source_data as (
        select *
        from {{ source('sap_adw', 'address') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["addressid"]) }} as sk_address
            , addressid
            , city
            , stateprovinceid
        from source_data
    )

select *
from sk_key
