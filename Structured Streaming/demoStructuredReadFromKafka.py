from pyspark.sql import SparkSession



if __name__ == '__main__':
    ss = SparkSession.builder.appName("StructuredKafkaWordCount").getOrCreate()
    ss.sparkContext.setLogLevel('WARN')

    lines = ss \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", 'word_count') \
        .load()\
        .selectExpr("CAST(value AS STRING)")

    wordCounts = lines.groupBy('value').count()

    query = wordCounts.selectExpr("CAST(value AS STRING) as key", "CONCAT(CAST(value AS STRING), ':', CAST(count AS STRING)) as value")\
        .writeStream\
        .outputMode('complete')\
        .format('kafka')\
        .option('kafka.bootstrap.servers', 'localhost:9092')\
        .option('topic', 'word_count_result')\
        .option('checkpointLocation', 'file:///root/tmp/kafka-sink-cp')\
        .trigger(processingTime='8 seconds')\
        .start()

    query.awaitTermination()
