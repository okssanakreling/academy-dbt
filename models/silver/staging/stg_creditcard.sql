with
    source_data as(
        select *
        from {{ source('sap_adw', 'creditcard') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["creditcardid"]) }} as sk_creditcard
            , creditcardid
            , cardtype
            , cardnumber
        from source_data
    )

select *
from sk_key
