from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as f
import pyspark.sql.types as t


def init_spark():
    spark = (SparkSession.builder
             .master("local")
             .appName("Python")
             .config(conf=SparkConf())
             .getOrCreate())
    return spark


def create_dataframe(spark):
    data = [("Alice", 20), ("Bob", 30), ("Charlie", 40)]
    schema = t.StructType([t.StructField("Name", t.StringType(), True),
                           t.StructField("Age", t.IntegerType(), True)])
    df = spark.createDataFrame(data, schema)
    return df


if __name__ == '__main__':
    spark = init_spark()
    df = create_dataframe(spark)
    df.show()
