import os, shutil
from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, asc
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import TimestampType, StringType

TEST_DATA_DIR_SPARK = 'file:///root/tmp/testdata/'

if __name__ == '__main__':
    schema = StructType([
        StructField("eventTime", TimestampType(), True),
        StructField("action", StringType(), True),
        StructField("district", StringType(), True)
    ])

    ss = SparkSession.builder.appName("StructuredEmallPurchaseCount").getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    lines = ss.readStream.format("json").schema(schema).option("maxFilePerTrigger", 100).load(TEST_DATA_DIR_SPARK)

    windowDuration = '1 minutes' # windowDuration 决定统计数据的时间范围
    windowedCounts = lines.filter("action='purchase'").groupBy('district', window('eventTime', windowDuration)).count().sort(asc('window'))
    query = windowedCounts.writeStream.outputMode("complete").format("console").option('truncate','false').trigger(processingTime='10 seconds').start() # processingTime 决定该程序多久运行一次
    query.awaitTermination()
