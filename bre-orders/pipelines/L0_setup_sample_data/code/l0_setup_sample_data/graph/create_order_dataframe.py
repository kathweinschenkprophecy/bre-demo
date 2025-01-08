from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l0_setup_sample_data.config.ConfigStore import *
from l0_setup_sample_data.functions import *

def create_order_dataframe(spark: SparkSession) -> DataFrame:
    order_data = [(101, 1, "2025-01-03", 200), (102, 2, "2024-12-16", 150), (103, 3, "2024-12-15", 300),
                  (104, 1, "2024-01-10", 350), (105, 4, "2024-12-06", 50),]
    order_columns = ["order_id", "customer_id", "order_date", "order_amount"]
    out0 = spark.createDataFrame(order_data, order_columns)

    return out0
