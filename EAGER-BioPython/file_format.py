#from abc import ABCMeta, abstractmethod
#import pprint
import sys
import os
#sys.path.append('/work/00791/xwj/SeqVerify/py_packages/lib/python2.7/site-packages/')
import getpass
import numpy as np


import time
import sys
from pyspark import SparkConf, SparkContext

sys.path.append('/work/00791/xwj/SeqVerify/py_packages/')
from BCBio.GFF import GFFExaminer


from Bio import SeqIO
import sys
import os


class biofile:
    def __init__(self,fileName,fmt):
        self.fileName = fileName
        self.fmt = fmt
        
    def fileSize(self):
        return os.stat(self.fileName).st_size
        #print("\nFile size is",os.stat(self.fileName).st_size, "bytes")
        
    def check(self):
        input_handle = open(self.fileName, "rU")
        sequences = SeqIO.parse(input_handle, self.fmt)
        try:
#             if (sum(1 for x in sequences)==0):
#                 return None
            next_item = next(sequences)
        except StopIteration:
            return None
        except ValueError:
            return None
        else: 
            return self.fmt


# inherit Format
class fasta(biofile):
    def read(self,sc):
        input_1 = "file:"+ self.fileName
        minPartitions= 128
        file_1= sc.textFile(input_1,minPartitions= minPartitions)
        # index with row numbers
        file_1_index = file_1.zipWithIndex()
        length = file_1_index.count()
        #get id row
        id_row = file_1_index.filter(lambda x:x[0][0]=='>').map(lambda x: (x[1],str(x[0][1:])))
        # get row numbers eg. [1, 4, 8], length is 10
        id_pos = id_row.map(lambda a: a[0]).collect()
        # get a list of [1,1,1,4,4,4,4,8,8,8] by repeating [1,4,8,10]
        # get a list of [1,1,1,4,4,4,4,8,8,8] by repeating [1,4,8,10]
        l = np.repeat(id_pos, np.diff(id_pos+[length])).tolist()
#        x = id_pos+[length]
#        tmp = [[i]*(j-i) for i, j in zip(x[:-1], x[1:])]
#        l = [e for y in tmp for e in y]
        # index with row number with l
        tmp_list = "tmp_list"
        with open(tmp_list,'w') as f:
            for item in l:
                f.write("%s\n" % item)
        f.close()
        os.system ("hadoop fs -rm " + tmp_list)
        os.system("hadoop fs -put " + tmp_list + " .")
        id_index = sc.textFile(tmp_list,minPartitions= minPartitions).map(lambda x:int(x)).zipWithIndex()

        # get sequence rows
        seq_row = file_1_index.filter(lambda x:x[0][0]!='>')
        # to get a list of [(0,(1,seq1_part1)),(0,(2,seq1_part2)),(3,(4,seq2_part1)),...]
        seq_row_re_index=id_index.map(lambda a: (a[1],a[0])).join(seq_row.map(lambda a: (a[1],str(a[0])))).map(lambda x: (x[1][0],(x[0],x[1][1])))
        def concat_sorted(x): 
            x[1].sort()
            s = ''
            for a in x[1]:
                s = s + a[1]
            return (x[0],s)
        seq_row_comb = seq_row_re_index.groupByKey().mapValues(list).map(concat_sorted)
        id_seq = id_row.join(seq_row_comb).map(lambda a: a[1])
        if (len(id_seq.take(1)[0][0].split())!=1):
            id_seq = id_seq.map(lambda x: (x[0].split()[1],x[1]))
        id_seq_file_1 = id_seq.cache()
        count = id_seq_file_1.count()
        top_five = id_seq_file_1.take(5)
        print ('\nnumber of sequences = %i\n' % count)
        print ('First five sequences')
        print (top_five)
        return id_seq_file_1


class fastq(biofile):
    def read(self,sc):
        input_1 = "file:"+ self.fileName
#         startTime = time.time()
        minPartitions= 128
        file_1= sc.textFile(input_1,minPartitions= minPartitions)
        file_1_index = file_1.zipWithIndex()        
        x=file_1_index.filter(lambda x: x[1] % 4 == 0).map(lambda x: str(x[0][1:].split()[0])).zipWithIndex().map(lambda a: (a[1],a[0]))
        y=file_1_index.filter(lambda x: x[1] % 4 ==1).map(lambda x: str(x[0])).zipWithIndex().map(lambda a: (a[1],a[0]))
        id_seq_file_1 = x.join(y).map(lambda a:a[1]).cache()
        count = id_seq_file_1.count()
        top_five = id_seq_file_1.take(5)
        print ('\nnumber of sequences = %i\n' % count)
        print ('First five sequences')
        print (top_five)
        return id_seq_file_1

class tab(biofile):
    pass

class gb(biofile):
    pass

class imgt(biofile):
    pass

class embl(biofile):
    pass

class seqxml(biofile):
    pass

class gff(biofile):
    def check(self):
        try:
            examiner = GFFExaminer()
            in_handle = open(self.fileName)
            examiner.available_limits(in_handle)
            #print("\nType of file detected:", "gff", "\n")
            in_handle.close()
            return "gff"
        except AssertionError: 
            return None

# fileName is a local path of a file
# return a file type string (e.g 'fasta' or 'fastq')
def checkFileFmt(fileName): 
    format_class = {"tab":tab, "fasta":fasta,"fastq":fastq,"gb":gb,"gff":gff, "seqxml":seqxml,"imgt":imgt} # embl format check is very slow
#    format_class = {"tab":tab, "fasta":fasta,"fastq":fastq,"gb":gb,"gff":gff,"seqxml":seqxml,"imgt":imgt,"embl":embl}
    for f in format_class.keys():
        #print(f)
        f_class = format_class[f]
        res = f_class(fileName,f).check()        
        if res:
#            f_class(fileName,f).fileSize()
            return res
        else:
            continue

# filePath is a local path of a file
# return an rdd of (id, seq)
def readIdSeq(filePath,sc):
    format_detected = checkFileFmt(filePath)
    # class
    f_class = eval(format_detected)
    # initial an instance
    f_object = f_class(filePath,format_detected)
    rdd = f_object.read(sc)
    return rdd



# rdd_1 (id,seq); rdd_2 (id,seq)
# return list_num with 5 numbers [ids in A not in B, ids in B not in A, ids in A and in B, identical id and sequence, identical id but different sequence]
def compareByKey(rdd_1,rdd_2):
    in_a_not_in_b = rdd_1.subtractByKey(rdd_2)
    in_b_not_in_a = rdd_2.subtractByKey(rdd_1)
    in_a_and_b    = rdd_1.join(rdd_2)
    equal_seqs = in_a_and_b.filter(lambda x: x[1][0]==x[1][1])
    diff_seqs  = in_a_and_b.subtractByKey(equal_seqs)
    a = in_a_not_in_b.count()
    b = in_b_not_in_a.count()
    c = in_a_and_b.count()
    d = equal_seqs.count()
    e = diff_seqs.count()
    path_in_a_not_in_b = "/user/" + getpass.getuser() +"/output/" + "in_a_not_in_b"
    path_in_b_not_in_a = "/user/" + getpass.getuser() +"/output/" + "in_b_not_in_a"
    path_equal_seqs = "/user/" + getpass.getuser() +"/output/" + "equal_seqs"
    path_diff_seqs = "/user/" + getpass.getuser() +"/output/" + "diff_seqs"
    os.system("hadoop fs -rm -r output")
    os.system("hadoop fs -ls")
    os.system("hadoop fs -mkdir output")
    in_a_not_in_b.saveAsTextFile(path_in_a_not_in_b)
    in_b_not_in_a.saveAsTextFile(path_in_b_not_in_a)
    equal_seqs.saveAsTextFile(path_equal_seqs)
    diff_seqs.saveAsTextFile(path_diff_seqs)
    print ("\nComparison output saved in: " + "/user/" + getpass.getuser() +"/output/")
    print("\nids in A not in B = %d \nids in B not in A = %d \nids in A and in B = %d \nidentical id and sequence = %d \nidentical id but different sequence = %d" % (a,b,c,d,e))
    return [a,b,c,d,e]


    

# rdd_1 (id,seq); rdd_2 (id,seq)
# return a a list of [rdd of (score, (seq_1, seq_2)), rdd_1, rdd_2], rdd_1 and rdd_2 will be reverse order if rev=True
def compareSeq(sc, rdd_1,rdd_2,rev=False):
    num_partition_a=16
    if rev:
        rdd_1 = reverseSeq(rdd_1)
        rdd_2 = reverseSeq(rdd_2)
    rdd_a = rdd_1.values().distinct()
    rdd_b = rdd_2.values().distinct()
    # match score for example abcdf abdc has matche score 2*2/(5+4)=0.44 
    def match_score (string_a, string_b):
        count = 0; i=0
        len_a = len(string_a); len_b = len(string_b)
        while i < len_a and i <  len_b:
            if (string_a[i] == string_b[i]):
                count = count +1
                i = i+1
            else:
                break
        return float(2*count)/float(len_a + len_b) 

    def last(iterator):
        e = ''
        for e in iterator:
            pass
        yield e


    # sortBy has to be after textFile to keep return same # of partitions 
    rdd_a_sorted = rdd_a.repartition(num_partition_a).sortBy(lambda x: x)
    last_a = filter(None,rdd_a_sorted.mapPartitions(last).collect())
    set_last_a = set(last_a)
    list_last_a = list(set_last_a)
    list_last_a.sort()
    first_a = rdd_a_sorted.first()
    # to deal with empty partitions, have to re-split rdd_a_sorted
    list_rdd = list()
    list_rdd.append(rdd_a_sorted.filter(lambda x: x>=first_a \
                                  and x<=list_last_a[0]).repartition(1).sortBy(lambda x: x))
    for i in xrange(len(list_last_a)-1):
        list_rdd.append(rdd_a_sorted.filter(lambda x: x>list_last_a[i] \
                                  and x<=list_last_a[i+1]).repartition(1).sortBy(lambda x: x))

    a_grouped_by_bound = sc.union(list_rdd)
    
    # start handling rdd_2
    rdd_b_sorted = rdd_b.repartition(num_partition_a*50).sortBy(lambda x: x)
    rdd_b_sorted_index = rdd_b_sorted.zipWithIndex().map(lambda x: (x[1],x[0]))
    len_rdd_b = rdd_b_sorted.count()

    # pos_high_match are positions that has highest match score, list_not_exit store string in list_last_a not in b
    pos_high_match = list();n =len(list_last_a); list_not_exist = list()


    for i in  xrange(n):
        tmp_max = rdd_b_sorted_index.map(lambda x:(match_score(list_last_a[i],x[1]),(x[0],list_last_a[i]))).max()
        if tmp_max[0] == 0:
            list_not_exist.append(tmp_max[1][1])
        else:
            pos_high_match.append(tmp_max[1][0])


    pos_not_exist = list()
    for e in list_not_exist:
        tmp = rdd_b_sorted_index.filter(lambda x: x[1]>e)
        if tmp.count()!=0:
            pos_not_exist.append(tmp.first()[0]-1)
        else:
            pos_not_exist.append(len_rdd_b-1)

    upper_bound_inclusive = pos_high_match + pos_not_exist + [-1]

    #if (len(pos_not_exist)!=len(list_not_exist)):upper_bound_inclusive.append(len_rdd_b-1)

    upper_bound_inclusive.sort()
    max_upper = max(upper_bound_inclusive)

    b_extra_large = list()
    # test if b has extra that is greater than max(upper_bound_inclusive)
    if max_upper != (len_rdd_b-1):
        b_extra_large = rdd_b_sorted_index.filter(lambda x: x[0]>max_upper).values()


    list_rdd = list()
    for i in xrange(len(upper_bound_inclusive)-1):
        list_rdd.append(rdd_b_sorted_index.filter(lambda x: x[0]>upper_bound_inclusive[i] \
                                  and x[0]<=upper_bound_inclusive[i+1]).values().repartition(1).sortBy(lambda x: x))

    b_grouped_by_bound = sc.union(list_rdd)

    def list_by_Partion(iterator): 
        yield (list(iterator))

    rdd_a_list = a_grouped_by_bound.mapPartitions(list_by_Partion)
    rdd_b_list= b_grouped_by_bound.mapPartitions(list_by_Partion)       


    rdd_a_b = rdd_a_list.zip(rdd_b_list)


    rdd_a_b_intersect = rdd_a_b.filter(lambda x: x[1])

    def matching_pair (string_list_a, string_list_b): # both lists are sorted
        len_a = len(string_list_a); len_b = len(string_list_b)
        i = 0; j = 0; score = 0
        dict = {};res = list()
        string_list_a = sorted(string_list_a)
        string_list_b = sorted(string_list_b)
        while (i < len_a and j <  len_b):
            a = string_list_a[i]; b = string_list_b[j] 
            if match_score(a,b) >= score:
                dict[a] = (b,match_score(a,b))
            if  a < b:
                i = i + 1
                score = 0
                res.append((dict[a][1],(a,dict[a][0])))
            elif dict[a][1]==1: 
                i = i + 1; j = j + 1
                score = 0
                res.append((dict[a][1],(a,dict[a][0])))
            else:
                if match_score(a,b) > score:
                    score = match_score(a,b)
                j = j + 1
        return res

    # flatMap flat nested list [[1,2],[3,4]] -> [1,2,3,4]
    res_tmp=rdd_a_b_intersect.flatMap(lambda x: matching_pair(x[0],x[1]))
    res=res_tmp.cache()
    print(res.take(5))
    return [res,rdd_1,rdd_2]

# list_num [ids in A not in B, ids in B not in A, ids in A and in B, identical id and sequence, identical id but different sequence]
# filePath_1: local file 1 path, e.g. /data/00791/xwj/5genos_fq/B73_all3_R1_val_1_test_4k.fq
# filePath_2: local file 2 path
# No return, standard output to console
def report(list_num,filePath_1,filePath_2):
    print ("\nFile A format:%s" % checkFileFmt(filePath_1))
    print ("File B format:%s" % checkFileFmt(filePath_2)+"\n")
    print ("\nComparison_Summary saved in current directory")
    file_name= "Comparison_Summary"
    with open(file_name,'w') as f:
        f.write("File A format:%s\n" % checkFileFmt(filePath_1))
        f.write("File B format:%s\n" % checkFileFmt(filePath_2))
        f.write("\nComparison by key output saved in hadoop file system: " + "/user/" + getpass.getuser() +"/output/")
        f.write("\nids in A not in B = %d \nids in B not in A = %d \nids in A and in B = %d \nidentical id and sequence = %d \nidentical id but different sequence = %d\n" % (list_num[0],list_num[1],list_num[2],list_num[3],list_num[4]))
        f.write("\nPrefix and suffix Comparison output files *.dist and *.count saved in current directory\n")
    f.close()  
    print("\nids in A not in B = %d \nids in B not in A = %d \nids in A and in B = %d \nidentical id and sequence = %d \nidentical id but different sequence = %d" % (list_num[0],list_num[1],list_num[2],list_num[3],list_num[4]))

# rdd_1 (id, seq); rdd_2 (id, seq); res_score_and_pair (score, (seq_1, seq_2)); 
# dist_1_name is the output file name of score ditribution; one column of match scores
# No retrun, write match scores to two files
def score_dist(rdd_1, rdd_2,res_score_and_pair):
    # rdd_1 distribution
    rdd_1_count = rdd_1.values().distinct().count()
    res_score_and_pair_count = res_score_and_pair.count()
    score = res_score_and_pair.keys().collect()
    zero_score_count_1 = rdd_1_count -res_score_and_pair_count
    zero_1 = [0]*zero_score_count_1
    score_1_out= score+zero_1
    # rdd_2 distribution
    rdd_2_count = rdd_2.values().distinct().count()
    zero_score_count_2 = rdd_2_count -res_score_and_pair_count
    zero_2 = [0]*zero_score_count_2
    score_2_out= score+zero_2
    return [score_1_out,score_2_out]
    

# path is a local file path of a file
# return file name
def getFileName(path):
    return os.path.basename(path)


# rdd_1 (id,seq)
# return a list of frequency of seqs
def seqDuplicateCount(rdd_1):
    duplicate_count= rdd_1.values().map(lambda x: (x,1)).reduceByKey(lambda a,b:a+b).values().collect()
    return duplicate_count

# a_list is a list of numbers; fileName is the output file name
# No return. Write to file in current directory
def writeListToFile(a_list, fileName):
    with open(fileName, 'w') as f:
        for item in a_list:
            f.write("%s\n" % item)

# rdd is (id,seq)
# return an rdd with seq string in reverse order
def reverseSeq(rdd):
    return rdd.map(lambda x: (x[0],x[1][::-1]))

def getDistinctWithIndex(rdd):
    rdd.values().distinct().zipWithIndex().map(lambda x: (x[1],x[0]))
    rdd.cache()
    return rdd


## rdd: (id,seq); select distince sequences and zip with index
## return 
## Note: function getWindowStartEndPos and getSubstringSeqWithPos used in map should define within getSubstringSeqWithPosRDD
def getSubstringSeqWithPosRDD(sc,rdd,seg_len,interval):
    ## get moving window start and end postion return list of (start_pos, end_pos)
    ## input s_len: string total length; seg_len:moving window segment length; interval: moving window step size
    ## return list of (start_pos, end_pos)
    def getWindowStartEndPos(s_len, seg_len,interval):
        if s_len < (seg_len+interval):
            print('segment or interal too big')
        elif interval > seg_len:
            print('interval too big')
        else:
    #        import numpy as np
            last_lower_bnd = s_len - seg_len
            tmp = np.linspace(0,last_lower_bnd/interval*interval,last_lower_bnd/interval,endpoint=False).tolist()
            start_pos = list(set([int(x) for x in tmp] +[last_lower_bnd/interval*interval,last_lower_bnd]))
            start_pos.sort()
            end_pos = [x + seg_len -1 for x in start_pos]
            return zip(start_pos,end_pos)

    ## get substring with list of segment indices
    ## input s: sequence string; id: sequence id; getWindowStartEndPos: pass-in function, t: (s_len,seg_len,interval)
    ## return (sequence_string, (sequence_id, start_index, end_index ))
    def getSubstringSeqWithPos(s,id, getWindowStartEndPos, t):
        list_start_end = getWindowStartEndPos(*t)
        return [(s[x[0]:(x[1]+1)],(id, x[0],x[1])) for x in list_start_end]

    
    rdd_distinct = rdd
    res_rdd = rdd_distinct.flatMap(lambda x: getSubstringSeqWithPos(x[1],x[0],getWindowStartEndPos, (len(x[1]),seg_len,interval)))
    res_rdd.cache() # to cache the return rdd in memory, avoiding different results of multiple evaluation.
    res_rdd.take(10)
    return res_rdd


## input rdd (seq_string, (row_number, start, end)) 
## output rdd (seq_bitarray, (row_number, start, end))
def getBitarraySeqWithPosRDD(sc, rdd,bitarray_lib_path="/home/03076/rhuang/.local/lib/python2.7/site-packages"):
    ## string to bitarrry
    ## input string s; return a bitarray as encoded in d
    def stringToBitarray(s, bitarray_lib_path=bitarray_lib_path):
        sys.path.append(bitarray_lib_path)
        from bitarray import bitarray
        d = {'A':bitarray('000'), 'T':bitarray('001'),'C':bitarray('010'),'G':bitarray('011'),'N':bitarray('100')}
        b = bitarray()
        b.encode(d,s)
        return b
    
    rdd_res=rdd.map(lambda x: (stringToBitarray(x[0]),x[1]))
    return rdd_res

    
def getJoinRdd(sc, rdd_1, rdd_2 ,bitarray_lib_path="/home/03076/rhuang/.local/lib/python2.7/site-packages"):
    def stringToBitarray(s, bitarray_lib_path=bitarray_lib_path):
        sys.path.append(bitarray_lib_path)
        from bitarray import bitarray
        d = {'A':bitarray('000'), 'T':bitarray('001'),'C':bitarray('010'),'G':bitarray('011'),'N':bitarray('100')}
        b = bitarray()
        b.encode(d,s)
        return b
    
    sys.path.append(bitarray_lib_path)
    from bitarray import bitarray
    rdd_bit_1=rdd_1.map(lambda x: (stringToBitarray(x[0]),x[1]))
    rdd_bit_2=rdd_2.map(lambda x: (stringToBitarray(x[0]),x[1]))
    res = rdd_bit_1.join(rdd_bit_2)
    return res



    
