from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils

@dp.table
def customers_bronze():
    df=spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","csv")\
            .option("clodFiles.inferColumnTypes",True)\
                .option("header",True)\
                    .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
                        .load("/Volumes/bharat/sdp/source/customers").withColumn("ingestion_time",utils.get_current_ts())
    return df

@dp.table
def sales_bronze():
    df=spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","csv")\
            .option("clodFiles.inferColumnTypes",True)\
                .option("coludFiles.schemaEvolutionMode","addNewColumns")\
                    .option("header",True)\
                        .load("/Volumes/bharat/sdp/source/sales").withColumn("ingestion_time",utils.get_current_ts())

    return df

@dp.table
def products_bronze():
    df=spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","csv")\
            .option("clodFiles.inferColumnTypes",True)\
                .option("coludFiles.schemaEvolutionMode","addNewColumns")\
                    .option("header",True)\
                        .load("/Volumes/bharat/sdp/source/products").withColumn("ingestion_time",utils.get_current_ts())
    return df