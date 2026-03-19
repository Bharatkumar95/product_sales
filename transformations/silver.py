from pyspark import pipelines as dp
from pyspark.sql.functions import * 

dp.create_streaming_table("customers_silver")

dp.create_auto_cdc_flow(
target = "customers_silver",
source = "customers_bronze",
keys = ["customer_id"],
sequence_by = col("sequenceNum"),
apply_as_deletes = expr("operation = 'DELETE'"),
#apply_as_truncates = expr("operation = 'TRUNCATE'"),
except_column_list = ["operation", "sequenceNum", "_rescued_data","ingestion_time"],
stored_as_scd_type = 2
)


@dp.table
def customers_active_silver():
    return spark.sql("""select * from stream(customers_silver) where __END_AT IS NULL""")
    # df= spark.readStream.table("customers_silver")
    # df=df.filter(col("__END_AT").isNull())
    # return df

@dp.expect_all_or_drop({"null check":" order_id is not null and customer_id is not null and product_id is not null"})
@dp.table
def sales_silver():
    df= spark.readStream.table("bharat.sdp.sales_bronze")
    return df.withWatermark("ingestion_time", "1 day")\
        .dropDuplicates(["order_id"])

dp.create_streaming_table("products_silver")

dp.create_auto_cdc_flow(
target = "products_silver",
source = "products_bronze",
keys = ["product_id"],
sequence_by = col("seqNum"),
# apply_as_deletes = expr("operation = 'DELETE'"),
#apply_as_truncates = expr("operation = 'TRUNCATE'"),
except_column_list = ["operation", "seqNum", "_rescued_data","ingestion_time"],
stored_as_scd_type = 1
)


