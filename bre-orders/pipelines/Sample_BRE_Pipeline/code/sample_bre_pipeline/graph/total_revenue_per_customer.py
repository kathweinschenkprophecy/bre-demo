from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sample_bre_pipeline.config.ConfigStore import *
from sample_bre_pipeline.functions import *

def total_revenue_per_customer(spark: SparkSession, SchemaTransform_1: DataFrame) -> DataFrame:
    df1 = SchemaTransform_1.groupBy(col("customer_id"))

    return df1.agg(
        first(col("name")).alias("name"), 
        first(col("location")).alias("location"), 
        first(col("spending")).alias("spending"), 
        sum(col("order_amount")).alias("total_order_amount"), 
        first(col("last_purchase_date")).alias("last_purchase_date"), 
        first(col("is_recent_order")).alias("is_recent_order")
    )
