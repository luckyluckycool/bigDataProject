from pyspark.sql import SparkSession
import pyspark.sql.types as t
from pyspark.sql.functions import col

from workers.Aliases import *
import config


class FareWorker:
    fare_schema = t.StructType([
        t.StructField(medallion, t.StringType(), False),
        t.StructField(hack_license, t.StringType(), False),
        t.StructField(vendor_id, t.StringType(), False),
        t.StructField(pickup_datetime, t.TimestampType(), False),
        t.StructField(payment_type, t.StringType(), False),
        t.StructField(fare_amount, t.FloatType(), False),
        t.StructField(surcharge, t.FloatType(), False),
        t.StructField(mta_tax, t.FloatType(), False),
        t.StructField(tip_amount, t.FloatType(), False),
        t.StructField(tolls_amount, t.FloatType(), False),
        t.StructField(total_amount, t.FloatType(), False),
    ])

    preprocess_filter_condition = (
            (col(fare_amount) > 0)
            & (col(surcharge) > 0)
            & (col(mta_tax) > 0)
            & (col(tip_amount) > 0)
            & (col(tolls_amount) > 0)
            & (col(total_amount) > 0)
    )

    def __init__(self, spark: SparkSession):
        self.df = (spark.read
                   .csv(config.fare_dataset, self.fare_schema, header=True, nullValue='null')
                   .filter(self.preprocess_filter_condition))

    def show(self):
        self.df.show()
