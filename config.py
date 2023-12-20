from pyspark.sql import DataFrame, SparkSession
from pyspark import SparkConf

trip_dataset = 'dataset/trip_data'
fare_dataset = 'dataset/trip_fare'
output_folder = 'output'


def write_to_csv(df: DataFrame, filename: str) -> None:
    df.write.csv(output_folder + '/' + filename + '.csv', header=True, mode='overwrite')


def init_spark() -> SparkSession:
    return (SparkSession.builder
            .master("local")
            .appName("Python")
            .config(conf=SparkConf())
            .getOrCreate())
