with
    stg_vendor as (
        select *
        from {{ ref("stg_vendor") }}
    )

    , vendors as (
        select
            sk_vendor
            , name
        from stg_vendor
    )

select *
from vendors