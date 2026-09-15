from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.table(
    name="bronze_inventory",
    comment="Raw Inventory Data"
)
def load_inventory():

    inventory_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(
            "/Volumes/wns24082026/quickstart_schema/sandbox/datasets/e-commerce/staging/example_demo/"
        )
    )

    metadata_df = (
        inventory_df
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

    data_quality_df = (
        metadata_df
        .withColumn(
            "reorder_deadline_approaching",
            col("reorder_level") <= 4
        )
    )

    return data_quality_df


@dp.table(name="silver_inventory")
@dp.expect("product_id", "product_id IS NOT NULL")
def load_inventory_silver():

    df = (
        spark.readStream
        .table("bronze_inventory")
        .filter(col("reorder_level").isNotNull())
        .withColumn(
            "reorder_units",
            col("available_quantity") - col("reserved_quantity")
        )
    )

    return df


@dp.table(name="gold_orders")
def load_inventory_gold():

    df = (
        spark.readStream
        .table("silver_inventory")
        .filter(col("reorder_units") < 10)
        .groupBy("product_id")
        .count()
    )

    return df