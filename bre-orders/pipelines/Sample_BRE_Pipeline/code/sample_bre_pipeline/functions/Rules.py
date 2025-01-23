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
    return when((spending > lit(500)), struct(lit(1).alias("high_value_flag"), lit(None).alias("high_risk_flag")))\
        .when((total_order_amount > lit(500)), struct(lit(1).alias("high_value_flag"), lit(None).alias("high_risk_flag")))\
        .otherwise(struct(lit(0).alias("high_value_flag"), lit(None).alias("high_risk_flag")))\
        .alias("IdentifyHighValueCustomers", metadata = get_column_metadata())
