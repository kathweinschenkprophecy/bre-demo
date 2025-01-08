from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sample_bre_pipeline.config.ConfigStore import *
from sample_bre_pipeline.functions import *

def load_customers_data(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("customer_id", LongType(), True), StructField("name", StringType(), True), StructField("location", StringType(), True), StructField("age", LongType(), True), StructField("spending", LongType(), True), StructField("last_purchase_date", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/tables/Sample_Data/customers")
