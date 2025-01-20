from pyspark.sql.connect.session import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, FloatType, IntegerType

from mapping.FullDataColumns import pickup_datetime, payment_type_encoded, fare_amount, surcharge, mta_tax, tip_amount, \
    tolls_amount, total_amount, store_and_fwd_flag_binary, rate_code, dropoff_datetime, passenger_count, \
    trip_time_in_secs, trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude


class FullDataTable:
    full_data_schema = StructType([
        StructField(payment_type_encoded, IntegerType(), False),
        StructField(fare_amount, FloatType(), False),
        StructField(surcharge, FloatType(), False),
        StructField(mta_tax, FloatType(), False),
        StructField(tip_amount, FloatType(), False),
        StructField(tolls_amount, FloatType(), False),
        StructField(total_amount, FloatType(), False),
        StructField(rate_code, IntegerType(), False),
        StructField(store_and_fwd_flag_binary, IntegerType(), False),
        StructField(pickup_datetime, TimestampType(), False),
        StructField(dropoff_datetime, TimestampType(), False),
        StructField(passenger_count, IntegerType(), False),
        StructField(trip_time_in_secs, IntegerType(), False),
        StructField(trip_distance, FloatType(), False),
        StructField(pickup_longitude, FloatType(), False),
        StructField(pickup_latitude, FloatType(), False),
        StructField(dropoff_longitude, FloatType(), False),
        StructField(dropoff_latitude, FloatType(), False),
    ])

    def __init__(self, spark: SparkSession):
        self.df = spark.read.parquet('dataset/joined_df.parquet')