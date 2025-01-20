import pyspark.sql.types as t
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, avg, month, when

import config
from mapping.FareColumns import total_amount, fare_amount
from mapping.TripColumns import medallion, hack_license, vendor_id, rate_code, store_and_fwd_flag, pickup_datetime, \
    dropoff_datetime, passenger_count, trip_time_in_secs, trip_distance, pickup_longitude, pickup_latitude, \
    dropoff_longitude, dropoff_latitude, store_and_fwd_flag_binary


class TripTable:
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


    def __init__(self, spark: SparkSession):
        self.df: DataFrame = (spark.read
                              .csv(config.trip_dataset, self.trip_schema, header=True, ignoreLeadingWhiteSpace=True,
                                   ignoreTrailingWhiteSpace=True, nullValue='null')
                              .filter(((col(trip_distance) > 0)
                                       & (col(trip_distance) < 22)
                                       & (col(trip_time_in_secs) > 0)
                                       & (col(trip_time_in_secs) < 5400)
                                       & (col(rate_code) <= 6)
                                       & (col(passenger_count) <= 6)
                                       & (col(vendor_id) == 'CMT')
                                       ))
                              .drop(col(vendor_id))
                              .dropna()
                              .withColumn(store_and_fwd_flag_binary,
                                          when(col(store_and_fwd_flag) == 'Y', 1).otherwise(0))
                              .drop(col(store_and_fwd_flag))
                              )

    def show(self, n=None):
        self.df.show(n=n)
