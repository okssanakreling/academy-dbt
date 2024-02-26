with
    status_data as (
        select *
        from unnest(
            [
                struct(1 as salestatus, "In process" as salesstatusname)
                , (2, "Approved")
                , (3, "Backordered")
                , (4, "Rejected")
                , (5, "Shipped")
                , (6, "Cancelled")
            ]
        )
    )

    , status_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(["salestatus"]) }} as sk_status
            , *
        from status_data
    )

select *
from status_sk