from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sample_bre_pipeline.config.ConfigStore import *
from sample_bre_pipeline.functions import *

def customer_orders_details(spark: SparkSession, Customer: DataFrame, Orders: DataFrame, ) -> DataFrame:
    return Customer\
        .alias("Customer")\
        .join(Orders.alias("Orders"), (col("Customer.customer_id") == col("Orders.customer_id")), "inner")\
        .select(col("Orders.order_id").alias("order_id"), col("Orders.order_date").cast(DateType()).alias("order_date"), col("Orders.order_amount").alias("order_amount"), col("Customer.age").alias("age"), col("Customer.customer_id").alias("customer_id"), col("Customer.spending").alias("spending"), col("Customer.location").alias("location"), col("Customer.name").alias("name"), col("Customer.last_purchase_date").cast(DateType()).alias("last_purchase_date"))
