from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, length



if __name__ == '__main__':
    ss = SparkSession.builder.appName("StructuredNetworkCountFileSink").getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    dataFrame = ss.readStream.format("socket")\
        .option("host", "localhost")\
        .option("port", 9999)\
        .load()

    dataFrame2 = dataFrame.select(
        explode(
            split(dataFrame.value, " ")
        ).alias("word")
    )

    dataFrame3 = dataFrame2.filter(length("word")==5)

    query = dataFrame3.writeStream.outputMode("append").format("parquet").option("path", "file:///root/workspace/spark_structured/filesink").option("checkpointLocation", "file:///root/tmp/file-sink-cp").trigger(processingTime="8 seconds").start()
    query.awaitTermination()
