from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

input = sc.textFile("Recorder.txt")
outputFile = "outputPTA"
del_row = input.take(2)
# 提取键值对
input = input.filter(lambda line: line not in del_row).map(lambda line: line.split(" ")).filter(lambda line: int(line[2])>0)
counts0 = input.map(lambda pair: ((pair[0], pair[1]), pair[2])).reduceByKey(lambda a, b: max(a, b))
res0 = counts0.map(lambda x: list(x)).map(lambda x: (x[0][0], int(x[1]))).reduceByKey(lambda a, b: a + b)
# 填补缺失值
conn0 = counts0.keys().map(lambda x: x[0]).distinct()
conn1 = counts0.keys().map(lambda x: x[1]).distinct()
conn0 = conn0.cartesian(conn1).map(lambda pair: (pair, '_')).union(counts0).reduceByKey(lambda a, b: min(a, b))
# 合并
conn0 = conn0.map(lambda x: list(x)).sortByKey(ascending=True).map(lambda x: (x[0][0], x[1])
                                                                   ).groupByKey().map(lambda x: (x[0], list(x[1])))
res = conn0.rightOuterJoin(res0).sortBy(keyfunc=lambda x: x[1][1], ascending=False)

print(res.collect())
SparkContext.stop(())
