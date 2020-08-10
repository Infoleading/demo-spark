from pyspark import SparkConf, SparkContext
from pyspark.streaming import *



if __name__ == '__main__':

    conf = SparkConf().setAppName("WordsCountStreaming").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)

    linesDS = ssc.textFileStream("file:///root/workspace/spark_streaming/logfiles")
    countDS = linesDS.flatMap(lambda x:x.split(' '))\
        .map(lambda x:(x,1))\
        .reduceByKey(lambda a,b:a+b)
    countDS.pprint()

    ssc.start()
    ssc.awaitTermination()
