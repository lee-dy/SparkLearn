#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 参考链接 https://blog.csdn.net/allcovetalllose/article/details/78824337
'''
初始化SparkConf, SparkContext
从pyspark 导入SparkConf, SparkContext
'''
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

inputFile = "Sentences.txt"
outputFile = "output"

#读取我们的输入数据
input = sc.textFile(inputFile)
# 把它切分成一个个单词
words = input.flatMap(lambda line: line.split(" "))
#转换为键值对并计数
counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

#将统计出来的单词总数存入一个文本文件，引发求值
counts.repartition(1).saveAsTextFile(outputFile)

SparkContext.stop(())
