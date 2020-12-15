# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 newTry.py 

#### if stop writestream in kafka may have change checkpoint location and after if i want i can to reuse the previous one my main checkpoint location

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import tweepy
from textblob import TextBlob
from wordcloud import WordCloud
import pandas as pd
import numpy as np
import re
from pyspark.sql.functions import spark_partition_id, count as _count



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
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()
    # mainDf = df.selectExpr("*")
    # print(mainDf)


    print("Printing Schema of df: ")
    df.printSchema()

    df1 = df.selectExpr("CAST(value AS STRING)", "topic", "CAST(partition AS STRING)", "timestamp")
    # df1.show()
    


    # Define a schema for the transaction_detail data
    schema = StructType() \
        .add("sentence", StringType()) \
        .add("candidate", StringType()) 
     

    df2 = df1\
        .select(from_json(col("value"), schema).alias("transaction_detail"), "topic", "partition", "timestamp")


    df2.createOrReplaceTempView("main_table")

    df3 = spark.sql("select transaction_detail.*, topic, partition, timestamp from main_table ")

            ### Check Partitioning ###
    ########  Partitioning Restaurants ############
    df3 = df3.repartitionByRange(2, col("candidate"))
    # print(restaurants_partitioned.show())
    # print(restaurants_partitioned.rdd.getNumPartitions())
    # restaurants_partitioned.write.mode("overwrite").csv("apotelesmata/results.txt")

    print("-------- print the length of each partition  -------------------")
    df3_partitioning_details = df3.withColumn("partition_id", spark_partition_id()).groupBy("partition_id").agg(_count("candidate"))
    

    print_partitioning_details = df3_partitioning_details \
    .writeStream \
        .trigger(processingTime='15 seconds') \
    .outputMode("update") \
    .option("truncate", "false")\
    .format("console") \
    .start()

    # Simple aggregate - find total_transaction_amount by grouping transaction_card_type
    df4 = df3
       
    print("Printing Schema of df4: ")
    df4.printSchema()

    def cleanTxt(text):
        text = re.sub('@[A-Za-z0–9]+', '', text) #Removing @mentions
        text = re.sub('#', '', text) # Removing '#' hash tag
        text = re.sub('RT[\s]+', '', text) # Removing RT
        text = re.sub('https?:\/\/\S+', '', text) # Removing hyperlink
        # return text
        print('after cleaning text', text)


    ### Sentiment Analysis ###
    def get_sentiment_analysis_result(text):
        print('bikame!!! mesa !!!', text)
        tag = ''

        cleanTxt(text)

        # def getSubjectivity(text):
        subjectivity = TextBlob(text).sentiment.subjectivity
        print('subjectivity',subjectivity)
        # def getPolarity(text):
        polarity =  TextBlob(text).sentiment.polarity
        print('polarity',polarity)

        if polarity < 0:
            tag = 'Negative'
            return tag
        elif polarity == 0:
            tag = 'Neutral'
            return tag
        else:
            tag = 'Positive'
            return tag
        print(tag)


    get_sentiment_analysis_result_udf = udf(get_sentiment_analysis_result, StringType())
    df5 = df4.withColumn("sentiment_analysis_result", get_sentiment_analysis_result_udf(df4["sentence"]))  ### tin onomazw value giato sto write strem sto kafka prepei opwsdipote mia timi na einai value alliws sou leei not found value
    # df5.createOrReplaceTempView("df5")
    ### Sentiment Analysis ###

    # Write final result into console for debugging purpose
    trans_detail_write_stream = df5 \
        .writeStream \
            .trigger(processingTime='15 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    df5_toJson = df5.withColumn("value", to_json(struct([df5[x] for x in df5.columns])))
    print("!!!!!! print schema DF5 !!!!!!!!!!!!")
    df5.printSchema()

    trans_detail_write_stream_kafka = df5_toJson \
        .selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
        .trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .option("checkpointLocation", "file:///home//xkapotis//development//spark_scripts//read_from_kafka//checkpoint") \
        .start()
        # /home/xkapotis/development/spark_scripts/read_from_kafka/pysparkScript.py

    trans_detail_write_stream.awaitTermination()
    trans_detail_write_stream_kafka.awaitTermination()

    print("PySpark Structured Streaming with Kafka Demo Application Completed.")