from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import DataFrame
import pyspark.sql.types as t
from workers import FareWorker, TripWorker


def init_spark() -> SparkSession:
    return (SparkSession.builder
            .master("local")
            .appName("Python")
            .config(conf=SparkConf())
            .getOrCreate())


def create_df_example(spark: SparkSession) -> DataFrame:
    data = [("Alice", 20), ("Bob", 30), ("Charlie", 50)]
    schema = t.StructType([t.StructField("Name", t.StringType(), True),
                           t.StructField("Age", t.IntegerType(), True)])
    return spark.createDataFrame(data, schema)


spark = init_spark()
# df = create_df_example(spark)

tripWorker = TripWorker.TripWorker(spark)
tripWorker.show_df()

