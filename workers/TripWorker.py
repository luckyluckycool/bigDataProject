from pyspark.sql import SparkSession
import pyspark.sql.types as t
from pyspark.sql.functions import col

from workers.Aliases import *
import config
from pyspark.sql import DataFrame


class TripWorker:
    trip_schema = t.StructType([
        t.StructField(medallion, t.StringType(), False),
        t.StructField(hack_license, t.StringType(), False),
        t.StructField(vendor_id, t.StringType(), False),
        t.StructField(rate_code, t.IntegerType(), False),
        t.StructField(store_and_fwd_flag, t.StringType(), False),
        t.StructField(pickup_datetime, t.TimestampType(), False),
        t.StructField(dropoff_datetime, t.TimestampType(), False),
        t.StructField(passenger_count, t.IntegerType(), False),
        t.StructField(trip_time_in_secs, t.IntegerType(), False),
        t.StructField(trip_distance, t.FloatType(), False),
        t.StructField(pickup_longitude, t.FloatType(), False),
        t.StructField(pickup_latitude, t.FloatType(), False),
        t.StructField(dropoff_longitude, t.FloatType(), False),
        t.StructField(dropoff_latitude, t.FloatType(), False),
    ])

    preprocess_filter_condition = ((col(passenger_count) > 0) & (col(passenger_count) <= 9)
                                   & (col(trip_distance) > 0) & (col(trip_distance) < 70_000)
                                   & (col(trip_time_in_secs) > 0) & (col(trip_time_in_secs) < 60_000))

    def __init__(self, spark: SparkSession):
        self.df: DataFrame = (spark.read
                              .csv(config.trip_dataset, self.trip_schema, header=True, nullValue='null')
                              .filter(self.preprocess_filter_condition))

    def show(self, n=None):
        self.df.show(n=n)

    def count(self) -> int:
        return self.df.count()

    def schema(self):
        self.df.printSchema()
