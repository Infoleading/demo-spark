from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

def main():
    df_people = ss.read.json("file:///root/workspace/spark_SQL/people.json")

    df_people.printSchema()
    df_people.show()

    df_people2 = df_people.select("age")
    df_people2.show()

    df_people3 = df_people.filter(df_people["age"]<30)
    df_people3.show()

    gd_people = df_people.groupBy("age")
    gd_people.count().show()

    df_people4 = df_people.sort(df_people["age"].desc(), df_people["name"].asc())
    df_people4.show()

if __name__ == '__main__':
    ss = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    main()
