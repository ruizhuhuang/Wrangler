
# coding: utf-8

# In[2]:

import time
import sys
from pyspark import SparkConf, SparkContext

conf = (SparkConf()
         .setMaster("yarn-client")
         .set("spark.driver.maxResultSize", "80g")
         .setAppName("test-sequence")
)

sc = SparkContext(conf = conf)

input_1 = sys.argv[1]
startTime = time.time()
file_1= sc.textFile(input_1,minPartitions= 2000)
file_1_index = file_1.zipWithIndex()

def getId(x):
    if (x[1] % 4) == 0:
        return str(x[0][1:].split()[0])

def getSeq(x):
    if (x[1] % 4) == 1:
        return str(x[0])

x=file_1_index.map(getId).mapPartitions(lambda a: [x for x in a if x is not None]).zipWithIndex().map(lambda a: (a[1],a[0]))
y=file_1_index.map(getSeq).mapPartitions(lambda a: [x for x in a if x is not None]).zipWithIndex().map(lambda a: (a[1],a[0]))


id_seq_file_1 = x.join(y).sortByKey().map(lambda a:a[1])

print(id_seq_file_1.take(5))
print(id_seq_file_1.count())
print("total time = %f" % (time.time()-startTime))




# In[ ]:

#get_ipython().system(u'ipython nbconvert --to script read_fq_spark.ipynb')


# In[ ]:



