# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 newTry.py 

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import tweepy
from textblob import TextBlob
from wordcloud import WordCloud
import pandas as pd
import numpy as np
import re




KAFKA_TOPIC_NAME_CONS = "baidentopic,trumptopic"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "sentimentanalysis"
KAFKA_BOOTSTRAP_SERVERS_CONS = "127.0.0.1:9092"

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .config("spark.jars", "file://usr/local/spark/spark-3.0.1-bin-hadoop2.7/jars/jar_files/spark-sql-kafka-0-10_2.12-3.0.1.jar,file://usr/local/spark/spark-3.0.1-bin-hadoop2.7/jars/jar_files/kafka-clients-2.4.1.jar") \
        .config("spark.executor.extraClassPath", "file://usr/local/spark/spark-3.0.1-bin-hadoop2.7/jars/jar_files/spark-sql-kafka-0-10_2.12-3.0.1.jar:file://usr/local/spark/spark-3.0.1-bin-hadoop2.7/jars/jar_files/kafka-clients-2.4.1.jar") \
        .config("spark.executor.extraLibrary", "file://usr/local/spark/spark-3.0.1-bin-hadoop2.7/jars/jar_files/spark-sql-kafka-0-10_2.12-3.0.1.jar:file://usr/local/spark/spark-3.0.1-bin-hadoop2.7/jars/jar_files/kafka-clients-2.4.1.jar") \
        .config("spark.driver.extraClassPath", "file://usr/local/spark/spark-3.0.1-bin-hadoop2.7/jars/jar_files/spark-sql-kafka-0-10_2.12-3.0.1.jar:file://usr/local/spark/spark-3.0.1-bin-hadoop2.7/jars/jar_files/kafka-clients-2.4.1.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from testtopic
 
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of df: ")
    df.printSchema()

    df1 = df.selectExpr("CAST(value AS STRING)", "timestamp")
    df1.printSchema()


    # # Define a schema for the transaction_detail data
    schema = StructType() \
        .add("candidate", StringType())  \
            .add("sentiment_analysis_result", StringType())
     

    df2 = df1\
        .select(from_json(col("value"), schema).alias("transaction_detail"), "timestamp")

    df2.createOrReplaceTempView("main_table")

    df3 = spark.sql("select transaction_detail.*, timestamp from main_table ")
    df3.printSchema()

    df4 = df3
       
    # print("Printing Schema of df4: ")
    df4.printSchema()


    # # Write final result into console for debugging purpose
    # # .trigger(processingTime='15 seconds') \
    trans_detail_write_stream = df4 \
        .writeStream \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    # # Write key-value data from a DataFrame to a specific Kafka topic specified in an option

    trans_detail_write_stream.awaitTermination()

    # print("PySpark Structured Streaming with Kafka Demo Application Completed.")