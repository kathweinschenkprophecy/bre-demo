from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l0_setup_sample_data.config.ConfigStore import *
from l0_setup_sample_data.functions import *

def create_sample_customer_data(spark: SparkSession) -> DataFrame:
    # Sample Customer Data
    customer_data = [(1, "Alice", "New York", 25, 800, "2024-01-01"), (2, "Bob", "Los Angeles", 30, 300, "2023-12-15"),
                     (3, "Charlie", "Chicago", 35, 700, "2023-11-01"),
                     (4, "David", "Seattle", 28, 100, "2024-12-05"),
                     (5, "Eve", "San Francisco", 40, 900, "2023-10-10"),]
    customer_columns = ["customer_id", "name", "location", "age", "spending", "last_purchase_date"]
    out0 = spark.createDataFrame(customer_data, customer_columns)

    return out0
