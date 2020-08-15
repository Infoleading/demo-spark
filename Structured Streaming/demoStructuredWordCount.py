from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode






if __name__ == "__main__":
    ss = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()
    ss.sparkContext.setLogLevel('WARN')
    lines = ss.readStream.format("socket").option("host", "localhost").option("port","9999").load()
    words = lines.select(explode(split(lines.value, " ")).alias("word"))
    wordCounts = words.groupBy("word").count()

    query = wordCounts.writeStream.outputMode("complete").format("console").trigger(processingTime="8 seconds").start()
    query.awaitTermination()
