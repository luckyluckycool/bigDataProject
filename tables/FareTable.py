import pyspark.sql.types as t
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, avg, min, max, sum, month

import config
from mapping.FareColumns import medallion, hack_license, vendor_id, pickup_datetime, payment_type, fare_amount, \
    surcharge, mta_tax, tip_amount, tolls_amount, total_amount, payment_type_encoded


class FareTable:
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

    indexer = StringIndexer(inputCol=payment_type, outputCol=payment_type_encoded)

    def __init__(self, spark: SparkSession):
        self.df = (spark.read
                   .csv(config.fare_dataset, self.fare_schema, header=True, ignoreTrailingWhiteSpace=True,
                        ignoreLeadingWhiteSpace=True, nullValue='null')
                   .filter((col(fare_amount) > 0)
                           & (col(fare_amount) < 80)
                           & (col(surcharge) >= 0)
                           & (col(mta_tax) >= 0)
                           & (col(tip_amount) >= 0)
                           & (col(tolls_amount) >= 0)
                           & (col(total_amount) > 0)
                           & (col(total_amount) < 90)
                           & (col(vendor_id) == 'CMT')
                           )
                   .dropna()
                   .drop(col(vendor_id))
                   )

    def show(self):
        self.df.show()
