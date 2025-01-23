from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *

@execute_rule
def IdentifyRecentOrders(order_date: Column=lambda: col("order_date")):
    return when((order_date >= date_sub(current_date(), 30)), lit(1)).otherwise(lit(0)).alias("is_recent_order")

@execute_rule
def IdentifyHighValueCustomers(
        spending: Column=lambda: col("spending"), 
        total_order_amount: Column=lambda: col("total_order_amount")
):
    return when((spending > lit(500)), lit(1))\
        .when((total_order_amount > lit(500)), lit(1))\
        .otherwise(lit(0))\
        .alias("high_value_flag")
