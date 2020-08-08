from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("WriteHBase")
sc = SparkContext(conf=conf)
host = 'master'
table = 'student'
keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
conf = {"hbase.zookeeper.quorum": host,"hbase.mapred.outputtable": table,"mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat","mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable","mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
rawData = ['3,info,name,Rongcheng','3,info,gender,M','3,info,age,26','4,info,name,Guanhua','4,info,gender,M','4,info,age,27']
sc.parallelize(rawData).map(lambda x: (x[0],x.split(','))).saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)
