
# coding: utf-8

# In[ ]:

import time
import sys
from pyspark import SparkConf, SparkContext
import numpy as np

conf = (SparkConf()
         .setMaster("yarn-client")
         .set("spark.driver.maxResultSize", "50g")
         .setAppName("test-sequence")
)

sc = SparkContext(conf = conf)

input_1 = sys.argv[1]
startTime = time.time()
file_1= sc.textFile(input_1,minPartitions= 1000)
file_1_index = file_1.zipWithIndex()
length = file_1_index.count()

def getIdRow(x):
    if x[0][0]=='>':
        return (x[1],str(x[0][1:]))

def getSeqRow(x):
    if x[0][0]!='>':
        return x

id_row = file_1_index.map(getIdRow).mapPartitions(lambda a: [x for x in a if x is not None])

id_pos = id_row.map(lambda a: a[0]).collect()
l = np.repeat(id_pos, np.diff(id_pos+[length])).tolist()
id_index = sc.parallelize(l).zipWithIndex()

seq_row=file_1_index.map(getSeqRow).mapPartitions(lambda a: [x for x in a if x is not None])

seq_row_re_index=id_index.map(lambda a: (a[1],a[0])).join(seq_row.map(lambda a: (a[1],str(a[0])))).map(lambda x: (x[1][0],(x[0],x[1][1])))


def f(x): 
    x[1].sort()
    s = ''
    for a in x[1]:
        s = s + a[1]
    return (x[0],s)

seq_row_comb = seq_row_re_index.groupByKey().mapValues(list).map(f)

id_seq = id_row.join(seq_row_comb).map(lambda a: a[1])


def getSRA_fasta_IdSeq(x):
    return (x[0].split()[1],x[1])

id_seq_file_1 = id_seq.map(getSRA_fasta_IdSeq)

sample = id_seq_file_1.take(10)
n=id_seq_file_1.count()
print(sample)
print(n)
print("total time = %f" % (time.time()-startTime))


# In[1]:



# In[ ]:



