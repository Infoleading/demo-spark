
from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.streaming import *
import sys


if __name__ == '__main__':
    if len(sys.argv)!=3:
        print("Usage: NetworkWordCount.py <hostname> <port>", file=sys.stderr)
        exit(-1)

    conf = SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)

    linesDS = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    countDS = linesDS.flatMap(lambda x:x.split(' '))\
        .map(lambda x:(x,1))\
        .reduceByKey(lambda a,b:a+b)

    countDS.pprint()

    ssc.start()
    ssc.awaitTermination()
