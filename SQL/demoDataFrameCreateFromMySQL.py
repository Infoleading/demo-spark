from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *

def main():
    jdbcDF = ss.read.format("jdbc")\
        .option("driver","com.mysql.cj.jdbc.Driver")\
        .option("url","jdbc:mysql://localhost:3306/spark")\
        .option("dbtable","student")\
        .option("user","spark")\
        .option("password","spark")\
        .load()
    jdbcDF.show()

if __name__ == '__main__':
    ss = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    main()


