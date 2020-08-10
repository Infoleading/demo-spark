import time
from pyspark import SparkConf, SparkContext
from pyspark.streaming import *

if __name__ == '__main__':
    conf = SparkConf().setAppName("WordsCountStreaming").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)

    rddList = []
    for i in range(5):
        rddList = [sc.parallelize([j for j in range(1,1001)], 10)]
        time.sleep(1)

    queueDS = ssc.queueStream(rddList)
    resultDS = queueDS.map(lambda x:(x%10, 1)).reduceByKey(lambda a,b:a+b).pprint()

    ssc.start()
    ssc.stop(stopSparkContext=True, stopGraceFully=True)
