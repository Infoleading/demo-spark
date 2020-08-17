from pyspark.sql import SparkSession


if __name__ == '__main__':
    ss = SparkSession.builder.appName("TestRateStreamSource").getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    dataFrame = ss.readStream.format("rate").option('rowPerSecond', 5).load()
    print(dataFrame.schema)

    dataStreamWriter = dataFrame.writeStream\
        .outputMode("update")\
        .format("console")\
        .option("truncate", "false")\
        .start()

    dataStreamWriter.awaitTermination()
