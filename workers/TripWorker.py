from pyspark.sql import SparkSession
import pyspark.sql.types as t

import config


class TripWorker:
    trip_schema = t.StructType([
        t.StructField("medallion", t.StringType(), False),
        t.StructField("hack_license", t.StringType(), False),
        t.StructField("vendor_id", t.StringType(), False),
        t.StructField("rate_code", t.IntegerType(), False),
        t.StructField("store_and_fwd_flag", t.BooleanType(), False),
        t.StructField("pickup_datetime", t.TimestampType(), False),
        t.StructField("dropoff_datetime", t.TimestampType(), False),
        t.StructField("passenger_count", t.IntegerType(), False),
        t.StructField("trip_time_in_secs", t.IntegerType(), False),
        t.StructField("trip_distance", t.FloatType(), False),
        t.StructField("pickup_longitude", t.FloatType(), False),
        t.StructField("pickup_latitude", t.FloatType(), False),
        t.StructField("dropoff_longitude", t.FloatType(), False),
        t.StructField("dropoff_latitude", t.FloatType(), False),
    ])

    def __init__(self, spark: SparkSession):
        self.df = spark.read.csv(config.trip_dataset, self.trip_schema, header=True, nullValue='null')
        self.df = self.df

    def show(self):
        self.df.show()

    def count(self) -> int:
        return self.df.count()


