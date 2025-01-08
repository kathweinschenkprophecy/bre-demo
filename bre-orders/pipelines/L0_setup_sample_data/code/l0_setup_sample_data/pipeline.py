from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l0_setup_sample_data.config.ConfigStore import *
from l0_setup_sample_data.functions import *
from prophecy.utils import *
from l0_setup_sample_data.graph import *

def pipeline(spark: SparkSession) -> None:
    df_create_sample_customer_data = create_sample_customer_data(spark)
    df_create_order_dataframe = create_order_dataframe(spark)
    Customer(spark, df_create_sample_customer_data)
    Orders(spark, df_create_order_dataframe)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("L0_setup_sample_data")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/L0_setup_sample_data")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/L0_setup_sample_data", config = Config)(pipeline)

if __name__ == "__main__":
    main()
