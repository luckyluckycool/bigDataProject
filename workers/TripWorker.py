from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
from pyspark.sql.functions import col, avg, month
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
                              .csv(config.trip_dataset, self.trip_schema, header=True, ignoreLeadingWhiteSpace=True,
                                   ignoreTrailingWhiteSpace=True, nullValue='null')
                              .filter(self.preprocess_filter_condition))

    def show(self, n=None):
        self.df.show(n=n)

    def count(self) -> int:
        return self.df.count()

    def schema(self):
        self.df.printSchema()

    def trip_and_average_trip_over_100_dollars(self, fare_df: DataFrame) -> DataFrame:
        """
        Get trip info and average trip time, where total_amount >= 100$

        :parameter self: TripWorker instance,
        :parameter fare_df: pyspark.sql.DataFrame

        :returns: pyspark.sql.DataFrame:
        The DataFrame which contains columns 'medallion', 'hack_license', 'vendor_id',
        'trip_time_in_secs' and 'average_trip_time_over_100'
        """
        window = Window.orderBy(trip_time_in_secs).partitionBy(col('a.hack_license'), col('a.medallion'))
        return (self.df.alias('a')
                .join(fare_df.alias('b'),
                      (col('a.hack_license') == col('b.hack_license'))
                      & (col('a.pickup_datetime') == col('b.pickup_datetime')),
                      how='inner')
                .filter(col(total_amount) >= 100)
                .withColumn('average_trip_time_over_100', avg(col(trip_time_in_secs)).over(window))
                .select(col("a.medallion"), col('a.hack_license'), col('a.vendor_id'), trip_time_in_secs,
                        'average_trip_time_over_100'))

    def average_fare_amount_by_rate_in_summer(self, fare_df: DataFrame):
        """
        Get average fare amount by rate code in summer period

        :parameter self: TripWorker instance,
        :parameter fare_df: pyspark.sql.DataFrame

        :returns: pyspark.sql.DataFrame:
        The DataFrame which contains columns 'rate_code' and 'avg_fare_amount_by_type'
        """
        return (self.df
                .filter(month(col('pickup_datetime')).between(6, 8))
                .join(fare_df,
                      (self.df[hack_license] == fare_df[hack_license])
                      & (self.df[pickup_datetime] == fare_df[pickup_datetime]),
                      how='inner')
                .groupBy(col(rate_code))
                .agg(avg(col(fare_amount)).alias('avg_fare_amount_by_type'))
                .orderBy(col('avg_fare_amount_by_type')))

    def trip_distance_by_passenger_count_dependency(self):
        """
        Get average trip distance by passenger count

        :parameter self: TripWorker instance,

        :returns: pyspark.sql.DataFrame:
        The DataFrame which contains columns 'passenger_count' and 'trip_distance_by_passenger_count_dependency'
        """
        return (self.df
                .groupBy(col(passenger_count))
                .agg(avg(col(trip_distance)).alias('trip_distance_by_passenger_count_dependency'))
                .orderBy(col('trip_distance_by_passenger_count_dependency'))
                )
