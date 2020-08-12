from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pymysql
import sys

def updateFunc(new_values, last_sum):
    return sum(new_values)+(last_sum or 0)

def dbfunc(records):
    db = pymysql.connect("localhost", "spark", "spark", "spark") # 主机名 用户名 密码 库名
    cursor = db.cursor()
    def doInsert(p):
        sql = "insert into wordcount(word,count) values('%s','%s')"%(str(p[0]), str(p[1]))
        try:
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()
    for item in records:
        doInsert(item)

def func(rdd):
    repartitionRDD = rdd.repartition(3)
    repartitionRDD.foreachPartition(dbfunc)

if __name__ == '__main__':
    if len(sys.argv)!=3:
        print("Usage: SaveToTextFile.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="StreamingSaveToFile")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("file:///root/tmp")

    initStateRDD = sc.parallelize([(u'hello', 1),(u'world', 1),(u'to', 1)])

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    running_counts = lines.flatMap(lambda line:line.split(" "))\
        .map(lambda word:(word, 1))\
        .updateStateByKey(updateFunc=updateFunc, initialRDD=initStateRDD)

    running_counts.pprint()
    running_counts.foreachRDD(func)
    ssc.start()
    ssc.awaitTermination()
