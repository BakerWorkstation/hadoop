#!/usr/bin/env python
# -*- coding:UTF-8 -*-

from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
 
spark=SparkSession.builder.appName("abv").getOrCreate() #创建spark对象
print('spark对象已创建')
host = 'master01.hadoop.io'
table = 'student'
conf = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}
keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
hbase_rdd = spark.sparkContext.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat","org.apache.hadoop.hbase.io.ImmutableBytesWritable","org.apache.hadoop.hbase.client.Result",keyConverter=keyConv,valueConverter=valueConv,conf=conf)
print('+' * 50)
print('ready')
print('+' * 50)
count = hbase_rdd.count()
hbase_rdd.cache()
output = hbase_rdd.collect()
for (k, v) in output:
        print (k, v)
