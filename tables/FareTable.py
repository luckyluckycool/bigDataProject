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

    def avg_total_amount_by_payment_type(self):
        """
        Get average total price for trip by payment type

        :parameter self: FareWorker instance

        :returns: pyspark.sql.DataFrame: The DataFrame which contains columns
        'payment type' with each payment type and
        'avg_total_amount_by_payment_type' with calculated average total price for each payment type.
        """

        return (self.df
                .groupBy(col(payment_type))
                .agg(avg(total_amount).alias('avg_total_amount_by_payment_type'))
                .orderBy('avg_total_amount_by_payment_type'))

    def min_max_cache_tip(self):
        """
        Get minimum and maximum tip amount paid by cash

        :parameter self: FareWorker instance

        :returns: pyspark.sql.DataFrame:
        The DataFrame which contains columns 'min_tip_amount'
        with the smallest amount of tip and 'max_tip_amount' with
        the smallest amount of tip paided by cash

        """
        return (self.df
                .filter(col(payment_type) == 'CSH')
                .select(min(col(tip_amount)),
                        max(col(tip_amount))))

    def vendor_month_revenue(self):
        """
        Get DataFrame with all trips and vendors revenue per month

        :parameter self: FareWorker instance

        :returns: pyspark.sql.DataFrame:
        The DataFrame which contains columns 'medallion', 'hack_license', 'vendor', 'month' and 'total_revenue'
        """
        window = Window.orderBy(vendor_id, month(col(pickup_datetime))).partitionBy(vendor_id,
                                                                                    month(col(pickup_datetime)))
        return (self.df.withColumn('total_revenue', sum(col(total_amount)).over(window))
                .withColumn('month', month(col(pickup_datetime)))
                .select(medallion, hack_license, vendor_id, 'month', 'total_revenue'))
