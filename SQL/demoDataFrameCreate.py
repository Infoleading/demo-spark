from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

def main():
    df_people = ss.read.json("file:///root/workspace/spark_SQL/people.json")
    #df_people = ss.read.text("file:///root/workspace/spark_SQL/people.txt")
    df_people.show()

    #df_people.write.json("file:///root/workspace/spark_SQL/json")
    #df_people.write.text("file:///root/workspace/spark_SQL/text")
    df_people.write.format("json").save("file:///root/workspace/spark_SQL/people.out")

if __name__ == '__main__':
    ss = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    main()
