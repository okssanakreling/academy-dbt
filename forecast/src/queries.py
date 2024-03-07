import pandas_gbq


class Query:
    @property
    def query(self):
        ...

    def get_df(self):
        return pandas_gbq.read_gbq(self.query)


class ProductByStores(Query):
    @property
    def query(self):
        return r"""
            SELECT
                FORMAT_DATE('%Y%m%d', dim_dates.date) as date
                , stg_sales_territory.territoryid
                , COALESCE(stg_store.storeid, -1) as storeid
                , stg_product.productid
                , stg_sales_type.onlineorderflag
                , SUM(fact_sales_details.orderqty) as totalordered
            FROM gold.fact_sales
            JOIN gold.fact_sales_details
                ON fact_sales.sk_sale = fact_sales_details.fk_sale
            JOIN staging.stg_product
                ON fact_sales_details.fk_product = stg_product.sk_product
            LEFT JOIN staging.stg_store
                ON fact_sales.fk_store = stg_store.sk_store
            JOIN gold.dim_dates
                ON fact_sales.fk_orderdate = dim_dates.sk_date
            JOIN staging.stg_sales_type
                ON fact_sales.fk_type = stg_sales_type.sk_type
            JOIN staging.stg_sales_territory
                ON fact_sales.fk_territory = stg_sales_territory.sk_territory
            GROUP BY date, territoryid, storeid, productid, onlineorderflag
        """


class DatesAhead(Query):
    @property
    def query(self):
        return r"""
            WITH
                latest_sale AS (
                    SELECT
                        MAX(dim_dates.date) max_date
                    FROM gold.fact_sales
                    JOIN gold.dim_dates
                        ON fact_sales.fk_orderdate = dim_dates.sk_date
                )

                , dates AS (
                    SELECT
                        GENERATE_DATE_ARRAY(
                            latest_sale.max_date
                            , DATE_ADD(latest_sale.max_date, INTERVAL 90 DAY)
                            , INTERVAL 1 DAY
                        ) AS DATE
                    FROM latest_sale
                )

                , products AS (
                    SELECT DISTINCT
                        stg_sales_territory.territoryid
                        , COALESCE(stg_store.storeid, -1) as storeid
                        , stg_product.productid
                        , stg_sales_type.onlineorderflag
                    FROM gold.fact_sales
                    JOIN gold.fact_sales_details
                        ON fact_sales.sk_sale = fact_sales_details.fk_sale
                    JOIN staging.stg_product
                        ON fact_sales_details.fk_product = stg_product.sk_product
                    LEFT JOIN staging.stg_store
                        ON fact_sales.fk_store = stg_store.sk_store
                    JOIN staging.stg_sales_territory
                        ON fact_sales.fk_territory = stg_sales_territory.sk_territory
                    JOIN staging.stg_sales_type
                        ON fact_sales.fk_type = stg_sales_type.sk_type
                )

                SELECT
                    FORMAT_DATE('%Y%m%d', date) as date
                    , territoryid
                    , storeid
                    , productid
                    , onlineorderflag
                FROM dates, UNNEST(dates.date) as date
                CROSS JOIN products
        """
