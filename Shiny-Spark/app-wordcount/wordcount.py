import sys
from pyspark import SparkConf, SparkContext

input = sys.argv[1]
output = sys.argv[2]

conf = (SparkConf()
         .setMaster("yarn-client")
         .setAppName("My app")
         .set("spark.executor.memory", "1g")
	)
sc = SparkContext(conf = conf)

text_file = sc.textFile(input, minPartitions=10)
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (str(word), 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.cache()
res = counts.sortBy(lambda x: -x[1]).collect()
for e in res:
	print e[0],e[1]
#print(res)



#print(counts.count())
#print(counts.first())
#res.saveAsTextFile(output)
