from pyspark.sql import DataFrame, SparkSession

trip_dataset = 'dataset/trip_data'
fare_dataset = 'dataset/fare_data'
full_dataset = 'dataset/joined_df.parquet'
output_folder = 'output'


def write_to_csv(df: DataFrame, filename: str) -> None:
    df.write.csv(output_folder + '/' + filename + '.csv', header=True, mode='overwrite')


def init_spark() -> SparkSession:
    return (SparkSession.builder
            .master("local")
            .appName("Python")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "6g")
            .getOrCreate())
