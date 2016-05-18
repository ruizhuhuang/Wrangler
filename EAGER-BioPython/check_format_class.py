
# coding: utf-8


from abc import ABCMeta, abstractmethod
import pprint
import sys
import os
sys.path.append('/work/00791/xwj/SeqVerify/py_packages/lib/python2.7/site-packages/')



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
    pass

class fastq(biofile):
    pass

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
    def __init__(self,fileName,fmt):
        self.fileName = fileName
        self.fmt = fmt
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


# In[10]:

def checkFileFmt(fileName): 
    format_class = {"tab":tab, "fasta":fasta,"fastq":fastq,"gb":gb,"gff":gff,"seqxml":seqxml,"imgt":imgt,"embl":embl}
    for f in format_class.keys():
        #print(f)
        f_class = format_class[f]
        res = f_class(fileName,f).check()        
        if res:
            f_class(fileName,f).fileSize()
            return res
        else:
            continue
# fileNames={"cor6_6.tab","B73_all3_R1_val_1_test.fq","cor6_6.seqxml","cor6_6.imgt","cor6_6.embl","cor6_6.fasta","cor6_6.gb"}
# for file in fileNames:
#     print(file, checkFileFmt(file))


# In[6]:

#get_ipython().system(u'ipython nbconvert --to script check_format_class.ipynb')


# In[ ]:



