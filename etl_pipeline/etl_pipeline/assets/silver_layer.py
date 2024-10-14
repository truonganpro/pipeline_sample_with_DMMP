import pandas as pd
from dagster import asset, Output, AssetIn
from .bronze_layer import olist_orders_dataset, olist_order_items_dataset, olist_products_dataset, product_category_name_translation,olist_order_payments_dataset
@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    required_resource_keys={"mysql_io_manager"},
)
def dim_products(context, olist_products_dataset, product_category_name_translation):
    sql = """
    SELECT rp.product_id, pcnt.product_category_name_english
    FROM olist_products_dataset rp
    JOIN product_category_name_translation pcnt
    ON rp.product_category_name = pcnt.product_category_name
    """
    pd_data = context.resources.mysql_io_manager.extract_data(sql)
    # Lưu dữ liệu vào bảng fact_sales
    context.resources.mysql_io_manager.store_data(pd_data, table_name="dim_products")
    return Output(
        pd_data,
        metadata={"table": "dim_products", "records count": len(pd_data)},
    )

@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    required_resource_keys={"mysql_io_manager"},
)
def fact_sales(context, olist_orders_dataset, olist_order_items_dataset, olist_order_payments_dataset):
    sql = """
    SELECT ro.order_id, ro.customer_id, ro.order_purchase_timestamp, roi.product_id, rop.payment_value, ro.order_status
    FROM olist_orders_dataset ro
    JOIN olist_order_items_dataset roi ON ro.order_id = roi.order_id
    JOIN olist_order_payments_dataset rop ON ro.order_id = rop.order_id
    WHERE ro.order_status = 'delivered'
    """
    pd_data = context.resources.mysql_io_manager.extract_data(sql)

    # Lưu dữ liệu vào bảng fact_sales
    context.resources.mysql_io_manager.store_data(pd_data, table_name="fact_sales")

    return Output(
        pd_data,
        metadata={"table": "fact_sales", "records count": len(pd_data)},
    )
