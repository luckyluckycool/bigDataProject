import os

import pyspark.sql.types as t
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from config import init_spark
from tables.TripTable import TripTable

os.environ['HADOOP_HOME'] = 'C:/hadoop'
os.environ['PATH'] += os.pathsep + 'C:/hadoop/bin'


def create_df_example(spark: SparkSession) -> DataFrame:
    data = [("Alice", 20), ("Bob", 30), ("Charlie", 50)]
    schema = t.StructType([t.StructField("Name", t.StringType(), True),
                           t.StructField("Age", t.IntegerType(), True)])
    return spark.createDataFrame(data, schema)


spark = init_spark()
df = TripTable(spark)
df.show(5)
