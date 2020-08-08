from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row

def main():
    lines = sc.textFile("file:///root/workspace/spark_SQL/people.txt")
    lines.foreach(print)

    rdd1 = lines.filter(lambda line:len(line.strip())>0)
    rdd1.foreach(print)

    rdd2 = rdd1.map(lambda x:x.split(","))
    rdd2.foreach(print)

    rdd3 = rdd2.map(lambda p:Row(name=p[0], age=int(p[1])))
    rdd3.foreach(print)

    df_people = ss.createDataFrame(rdd3)
    df_people.show()

    df_people.createOrReplaceTempView("people")
    df_people2 = ss.sql("select * from people where age>20")
    df_people2.show()
    
    rdd4 = df_people2.rdd
    rdd4.foreach(print)

    rdd5 = rdd4.map(lambda p: "name:"+p.name+" age:"+str(p.age))
    rdd5.foreach(print)


if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("DataFrameFromRDD")
    sc = SparkContext(conf=conf)
    ss = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

    main()
