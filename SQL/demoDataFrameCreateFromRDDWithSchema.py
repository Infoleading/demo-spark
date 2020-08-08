from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *


def main():
    lines = sc.textFile("file:///root/workspace/spark_SQL/people.txt")
    rdd1 = lines.filter(lambda x: len(x.strip())>0)
    rdd1.foreach(print)

    rdd2 = rdd1.map(lambda x:x.split(","))
    rdd2.foreach(print)

    # 生成由 Row 组成的 RDD 数据
    rdd3 = rdd2.map(lambda x:Row(x[0], x[1]))
    rdd3.foreach(print)

    # 编程指定数据表元数据
    schemaString = "name age"
    fields = [StructField(fieldName, StringType(), True) for fieldName in schemaString.split(" ")]
    schema = StructType(fields)

    # 将RDD 与 schema 数据合并生成 DataFrame 对象
    df_people = ss.createDataFrame(rdd3, schema=schema)
    df_people.show()


if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("DataFrameFromRDD")
    sc = SparkContext(conf=conf)
    ss = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    main()
