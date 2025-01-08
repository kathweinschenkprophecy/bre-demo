from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sample_bre_pipeline.config.ConfigStore import *
from sample_bre_pipeline.functions import *
from prophecy.utils import *
from sample_bre_pipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_load_customers_data = load_customers_data(spark)
    df_load_orders_data = load_orders_data(spark)
    df_customer_orders_details = customer_orders_details(spark, df_load_customers_data, df_load_orders_data)
    df_AddRule_IdentifyRecentOrders = AddRule_IdentifyRecentOrders(spark, df_customer_orders_details)
    df_total_revenue_per_customer = total_revenue_per_customer(spark, df_AddRule_IdentifyRecentOrders)
    df_AddRule_IdentifyHighValueCustomers = AddRule_IdentifyHighValueCustomers(spark, df_total_revenue_per_customer)
    df_AddRule_PromoOffers = AddRule_PromoOffers(spark, df_AddRule_IdentifyHighValueCustomers)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Sample_BRE_Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Sample_BRE_Pipeline")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/Sample_BRE_Pipeline", config = Config)(pipeline)

if __name__ == "__main__":
    main()
