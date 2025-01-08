from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sample_bre_pipeline.config.ConfigStore import *
from sample_bre_pipeline.functions import *

def load_orders_data(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("order_id", LongType(), True), StructField("customer_id", LongType(), True), StructField("order_date", StringType(), True), StructField("order_amount", LongType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/tables/Sample_Data/orders")
