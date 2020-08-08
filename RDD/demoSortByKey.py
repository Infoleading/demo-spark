from pyspark import SparkConf, SparkContext

index = 0

def getindex():
    global index
    index+=1
    return index

def main():
    lines = sc.textFile("file:///root/files")
    result1 = lines.filter(lambda line:len(line.strip())>0)
    result2 = result1.map(lambda x:(int(x.strip()), ''))
    result3 = result2.repartition(1)
    result4 = result3.sortByKey()
    result5 = result4.map(lambda x:x[0])
    result6 = result5.map(lambda x:(getindex(), x))
    result6.foreach(print)
    result6.saveAsTextFile("file:///root/fileSorted")

if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("ReadHBase")
    sc = SparkContext(conf=conf)
    main()
