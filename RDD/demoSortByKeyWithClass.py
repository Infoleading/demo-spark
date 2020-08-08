from pyspark import SparkConf, SparkContext

index = 0

'''
自定义可排序对象
'''
class SecondarySortKey():
    def __init__(self, k):
        self.column1 = k[0]
        self.column2 = k[1]

    def __gt__(self, other):
        if other.column1 == self.column1:
            return self.column2>other.column2
        else:
            return self.column1>other.column1


def main():
    lines = sc.textFile("file:///root/secondarySort.txt")
    result1 = lines.filter(lambda line:len(line.strip())>0)
    result2 = result1.map(lambda x:((int(x.split(' ')[0]),int(x.split(' ')[1])), x))
    result3 = result2.map(lambda x:(SecondarySortKey(x[0]),x[1])) # 将自定义可排序对象作为 key
    result4 = result3.sortByKey(False) # sortByKey 利用自定义可排序对象对RDD元素进行排序
    result5 = result4.map(lambda x:x[1])
    result5.foreach(print)

if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("ReadHBase")
    sc = SparkContext(conf=conf)
    main()
