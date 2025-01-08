from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sample_bre_pipeline.config.ConfigStore import *
from sample_bre_pipeline.functions import *

def AddRule_IdentifyHighValueCustomers(spark: SparkSession, total_revenue_by_customer: DataFrame) -> DataFrame:
    return total_revenue_by_customer.withColumn(get_alias(IdentifyHighValueCustomers()), IdentifyHighValueCustomers())
