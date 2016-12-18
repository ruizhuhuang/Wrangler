import sys
from pyspark import SparkConf, SparkContext
import getpass
import numpy as np
import time

conf = (SparkConf()
         .setMaster("yarn-client")
         .set("spark.driver.maxResultSize", "50g")
         .setAppName("test-sequence")
)

sc = SparkContext(conf = conf)

from file_format import checkFileFmt
from file_format import readIdSeq
from file_format import compareByKey
from file_format import compareSeq
from file_format import report
from file_format import score_dist
from file_format import getFileName
from file_format import seqDuplicateCount
from file_format import writeListToFile
from file_format import reverseSeq


def main(argv):
    startTime = time.time()
    filePath_1 = argv[1] #"/data/00791/xwj/5genos_fq/B73_all3_R1_val_1_test_4k.fq"
    filePath_2 = argv[2] #"/data/00791/xwj/5genos_fq/SRR850328_1_test_3k.fasta"
    file_1_name = getFileName(filePath_1)
    file_2_name = getFileName(filePath_2)
    dist_1_name = file_1_name  + '.dist'
    dist_2_name = file_2_name + '.dist'
    dist_rev_1_name = file_1_name  + '.rev.dist'
    dist_rev_2_name = file_2_name + '.rev.dist'
    
    startRead = time.time()
    rdd_1 = readIdSeq(filePath_1,sc)
    rdd_2 = readIdSeq(filePath_2,sc)
    read_time = time.time() - startRead
    
    startCompareByKey = time.time()
    list_num = compareByKey(rdd_1,rdd_2)
    CompareByKey_time = time.time()-startCompareByKey
    
    startCompareBySeq = time.time()
    res_score_and_pair = compareSeq(sc, rdd_1, rdd_2, rev=False)
    # get match score distribution as a list
    score_out = score_dist(rdd_1=res_score_and_pair[1], rdd_2=res_score_and_pair[2],res_score_and_pair=res_score_and_pair[0])
    writeListToFile(score_out[0], dist_1_name)
    writeListToFile(score_out[1], dist_2_name)
    CompareBySeq_time = time.time() - startCompareBySeq
    
    
    startCompareByRevSeq = time.time()
    res_score_and_pair = compareSeq(sc, rdd_1, rdd_2, rev=True)
    # get match score distribution as a list
    score_out = score_dist(rdd_1=res_score_and_pair[1], rdd_2=res_score_and_pair[2],res_score_and_pair=res_score_and_pair[0])
    writeListToFile(score_out[0], dist_rev_1_name)
    writeListToFile(score_out[1], dist_rev_2_name)
    CompareByRevSeq_time = time.time() - startCompareByRevSeq
    
    startSeqDuplicateCount = time.time()
    res_score_and_pair[0].unpersist()
    writeListToFile(a_list=seqDuplicateCount(rdd_1),fileName=file_1_name+'.dup.count')
    writeListToFile(a_list=seqDuplicateCount(rdd_2),fileName=file_2_name+'.dup.count')
    
    report(list_num,filePath_1,filePath_2)
    SeqDuplicateCount_time = time.time() - startSeqDuplicateCount
    
    print ('\n read_time = %f' % read_time)
    print ('\n CompareByKey_time = %f' % CompareByKey_time)
    print ('\n CompareBySeq_time = %f' % CompareBySeq_time)
    print ('\n CompareByRevSeq_time = %f' % CompareByRevSeq_time)
    print ('\n SeqDuplicateCount_time = %f' % SeqDuplicateCount_time)
    print ('\n Total runtime = %f\n' % (time.time() - startTime))

if __name__ == "__main__":
    main(sys.argv)
