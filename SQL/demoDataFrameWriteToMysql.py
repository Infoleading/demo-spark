from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *

def main():
    studentRDD = sc.parallelize(["3 ZhangJin M 21","4 Wangwei F 26","5 Lijia M 38","6 Huhu F 33"])\
        .map(lambda x:x.split(' '))
    rowRDD = studentRDD.map(lambda p:Row(int(p[0].strip()), p[1].strip(), p[2].strip(), int(p[3].strip())))
    rowRDD.foreach(print)

    schema = StructType([StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                        StructField("gender", StringType(),True),
                        StructField("age", IntegerType(), True)])

    studentDF = ss.createDataFrame(rowRDD, schema=schema)
    studentDF.show()

    properDict = {'user':'spark', 'password':'spark', 'driver':'com.mysql.cj.jdbc.Driver'}
    studentDF.write.jdbc("jdbc:mysql://localhost/spark", "student", "append", properDict)

if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("DataFrameFromRDD")
    sc = SparkContext(conf=conf)
    ss = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    main()
