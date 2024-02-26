with
    sale_type as (
        select *
        from unnest(
            [
                struct(false as onlineorderflag, "Placed by sales person" as saletype)
                , (true, "Placed online by customer")
            ]
        )
    )

    , type_data as (
        select
            {{ dbt_utils.generate_surrogate_key(["onlineorderflag"]) }} as sk_type
            , onlineorderflag
            , saletype
        from sale_type
    )

select *
from type_data