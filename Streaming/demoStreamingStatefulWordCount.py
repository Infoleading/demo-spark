from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def updateFunc(new_values, last_sum):
    return sum(new_values)+(last_sum or 0)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: NetworkWordCountStateful.py <hostname> <port>", file=sys.stderr)
        exit(1)

    sc = SparkContext(appName="streaminStatefulWordCount")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("file:///root/tmp")

    initStateRDD = sc.parallelize([(u'hello', 1),(u'world', 1),(u'to', 1)])

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    running_counts = lines.flatMap(lambda line:line.split(" "))\
        .map(lambda word:(word, 1))\
        .updateStateByKey(updateFunc=updateFunc, initialRDD=initStateRDD)

    running_counts.pprint()
    ssc.start()
    ssc.awaitTermination()
