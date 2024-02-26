with 
    source_data as (
        select *
        from {{ source('sap_adw', 'workorder') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["workorderid"]) }} as sk_work_order
            , workorderid
            , productid
            , orderqty
            , scrappedqty
            , date(startdate) as startdate
            , date(enddate) as enddate
            , date(duedate) as duedate
        from source_data
    )

select *
from sk_key
