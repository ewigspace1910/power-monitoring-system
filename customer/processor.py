import json
import requests
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from schema import c_schema
import os

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--s", type=int, default=1, help="iterval, time(s) to split streams")
    parser.add_argument("--t", type=str, help="topic")
    parser.add_argument("--ip", type=str, default="localhost:9092", help="ip server:port")
    return parser.parse_args()


class Processor():
    def __init__(self, args):
        self.topic = args.t
        self.server = args.ip

    def run(self):
        spark = SparkSession \
        .builder \
        .appName("power-monitoring") \
        .master("local[*]") \
        .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.server) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "latest") \
            .load()

        print("Printing Schema of orders_df: ")
        df.printSchema()
        new_df = df.selectExpr("CAST(value AS STRING)", "timestamp")
        new_df = new_df.select(from_json(col('value'), c_schema).alias("consumption")) #{2015-01-12 00:24:00, NaN, 2}
        new_df = new_df.select("consumption.*")
        new_df.printSchema()

        #Transform
        new_df = new_df.groupby(col("resident_id")) \
                .agg({'grid_import':'sum'}).select("resident_id", col("sum(grid_import)"))

        # #write to output stream, then consumer kafka will send to power BI
        write_stream = new_df \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()
        write_stream.awaitTermination()
        
        # ds = new_df \
        #     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        #     .writeStream \
        #     .format("kafka") \
        #     .option("kafka.bootstrap.servers", self.server) \
        #     .option("topic", "bi-stream") \
        #     .start()
        # ds.awaitTermination()




if __name__ == "__main__":
    args = get_args()

    #init spark streaming processor
    processor = Processor(args)
    processor.run()