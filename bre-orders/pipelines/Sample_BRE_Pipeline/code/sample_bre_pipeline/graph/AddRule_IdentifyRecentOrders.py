from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sample_bre_pipeline.config.ConfigStore import *
from sample_bre_pipeline.functions import *

def AddRule_IdentifyRecentOrders(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(get_alias(IdentifyRecentOrders()), IdentifyRecentOrders())
