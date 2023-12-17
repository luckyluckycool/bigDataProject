from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
import pyspark.sql.types as t
import config


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


def create_dataframe(spark: SparkSession) -> DataFrame:
    df = spark.read.csv(config.path_to_dataset, header=True, nullValue='null')
    return df


if __name__ == '__main__':
    spark = init_spark()
    df = create_df_example(spark)
    df.show()
