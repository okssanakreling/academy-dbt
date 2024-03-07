with
    person as (
        select *
        from {{ ref("stg_person") }}
    )

    , customers as (
        select *
        from {{ ref("stg_customer") }}
    )

    , stg_emailaddress as (
        select *
        from {{ ref("stg_emailaddress") }}
    )

    , customer_data as (
        select
            customers.sk_customer as sk_customer
            , person.title
            , person.firstname
            , person.middlename
            , person.lastname
            , person.suffix
            , array_to_string(
                [
                    person.title
                    , person.firstname
                    , person.middlename
                    , person.lastname
                    , person.suffix
                ]
                , ' '
            ) as fullname
            , stg_emailaddress.emailaddress
        from person
        left join customers
            on customers.personid = person.personid
        left join stg_emailaddress
            on stg_emailaddress.personid = person.personid
        where customers.personid is not null
    )

select *
from customer_data
