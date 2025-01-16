from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from test.config.ConfigStore import *
from test.functions import *

def transform_schema_with_promocode(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(get_alias(PromoCodeRule()), PromoCodeRule()).drop("is_recent_order")
