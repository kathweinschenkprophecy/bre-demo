from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l0_setup_sample_data.config.ConfigStore import *
from l0_setup_sample_data.functions import *

def Customer(spark: SparkSession, create_sample_customer_data: DataFrame):
    create_sample_customer_data.write\
        .option("header", True)\
        .option("sep", ",")\
        .mode("overwrite")\
        .option("separator", ",")\
        .option("header", True)\
        .csv("dbfs:/FileStore/tables/Sample_Data/customers")
