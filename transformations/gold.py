from pyspark import pipelines as dp
from pyspark.sql.functions import * 

# total sales and discount amount for each active customer
@dp.materialized_view(schema=""" customer_id string, customer_name string , total_sales DOUBLE, total_discount DOUBLE""")
def total_sales_customer_gold():
    return spark.sql(""" select customer_id, customer_name, round(sum(total_amount),2) as total_sales, round(sum(discount_amount),2) as total_discount from (customers_active_silver) join (sales_silver) using(customer_id) group by customer_name , customer_id""")


# product_categorty with highest sales revenue

@dp.materialized_view()
def top_product_sale():
    return spark.table("products_silver").alias("a").join(spark.table("sales_silver").alias("b"),col("a.product_id")==col("b.product_id"), "inner").groupBy("product_category").agg(round(sum(col("total_amount")),2).alias("total_sales").cast("double")).select("product_category","total_sales").orderBy(col("total_sales").desc())