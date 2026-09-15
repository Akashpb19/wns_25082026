from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(name="bronze_customers",
           comment="Raw Customer Data")
def load_customers():
    df = spark.readStream.format("cloudFiles").option("cloudFiles.format","CSV").load(path="/Volumes/wns24082026/quickstart_schema/sandbox/datasets/e-commerce/staging/customers/").withColumn("_ingest_timestamp", expr("current_timestamp()")).withColumn("_source_file", col("_metadata.file_path"))
    return df

@dp.table(name="bronze_orders",
           comment="Raw Customer Data")
def load_orders():
    df = spark.readStream.format("cloudFiles").option("cloudFiles.format","JSON").load(path="/Volumes/wns24082026/quickstart_schema/sandbox/datasets/e-commerce/staging/orders/").withColumn("_ingest_timestamp", expr("current_timestamp()")).withColumn("_source_file", col("_metadata.file_path"))
    return df
@dp.expect_or_drop('valid_order_ts',"order_ts<=current_date()")
@dp.table(name="silver_orders")
def load_orders():
    df = spark.readStream.table("bronze_orders").filter(col("order_id").isNotNull()).withColumn("total_amount", col("qty")*col("price"))
    return df

@dp.table(name="gold_orders")
def load_orders():
    df = spark.readStream.table("silver_orders").groupBy("item_id").count()
    return df
