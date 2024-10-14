import pandas as pd
from dagster import asset, Output, AssetIn

# Asset gold layer - sales_values_by_category
@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "ecom"],
    ins={
        "fact_sales": AssetIn(key_prefix=["silver", "ecom"]),
        "dim_products": AssetIn(key_prefix=["silver", "ecom"]),
    },
    required_resource_keys={"mysql_io_manager"},
)
def sales_values_by_category(context, fact_sales: pd.DataFrame, dim_products: pd.DataFrame) -> Output[pd.DataFrame]:
    # SQL logic to create the final gold table
    sql = """
    WITH daily_sales_products AS (
        SELECT
            CAST(order_purchase_timestamp AS DATE) AS daily,
            fs.product_id,
            ROUND(SUM(CAST(fs.payment_value AS FLOAT)), 2) AS sales,
            COUNT(DISTINCT(fs.order_id)) AS bills
        FROM fact_sales fs
        WHERE fs.order_status = 'delivered'
        GROUP BY
            CAST(order_purchase_timestamp AS DATE),
            fs.product_id
    ),
    daily_sales_categories AS (
        SELECT
            dsp.daily,
            DATE_FORMAT(dsp.daily, 'y-MM') AS monthly,
            dp.product_category_name_english AS category,
            dsp.sales,
            dsp.bills,
            (dsp.sales / dsp.bills) AS values_per_bills
        FROM daily_sales_products dsp
        JOIN dim_products dp ON dsp.product_id = dp.product_id
    )
    SELECT
        monthly,
        category,
        SUM(sales) AS total_sales,
        SUM(bills) AS total_bills,
        (SUM(sales) * 1.0 / SUM(bills)) AS values_per_bills
    FROM daily_sales_categories
    GROUP BY
        monthly,
        category
    """

    # Extract data using updated fact_sales and dim_products
    pd_data = context.resources.mysql_io_manager.extract_data(sql)
    
    # Store the final result into the table
    context.resources.mysql_io_manager.store_data(pd_data, table_name="sales_values_by_category")

    return Output(
        pd_data,
        metadata={"table": "sales_values_by_category", "records count": len(pd_data)},
    )
