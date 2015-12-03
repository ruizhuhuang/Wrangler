
# coding: utf-8

# In[1]:

from abc import ABCMeta, abstractmethod
import pprint
from BCBio.GFF import GFFExaminer
from Bio import SeqIO
import sys
import os

class Format:
#    __metaclass__ = ABCMeta

#    @abstractmethod
    def __init__(self,fileName,fmt):
        self.fileName = fileName
        self.fmt = fmt
        
    def fileSize(self):
        print("\nFile size is",os.stat(self.fileName).st_size, "bytes")
        
    def check(self):
        input_handle = open(self.fileName, "rU")
        sequences = SeqIO.parse(input_handle, self.fmt)
        try:
            if (sum(1 for x in sequences)==0):
                return None
        except ValueError:
            return None
        else: 
            return self.fmt

class fasta(Format):
    pass

class gb(Format):
    pass

class imgt(Format):
    pass

class embl(Format):
    pass

class tab(Format):
    pass

class seqxml(Format):
    pass

class gff(Format):
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



# In[4]:

def checkFileFmt(fileName): 
    format_class = {"fasta":fasta,"gb":gb,"gff":gff,"tab":tab,"seqxml":seqxml,"imgt":imgt,"embl":embl}
    for f in format_class.keys():
        #print(f)
        f_class = format_class[f]
        res = f_class(fileName,f).check()        
        if res:
            f_class(fileName,f).fileSize()
            return res
        else:
            continue
# fileNames={"cor6_6.tab","cor6_6.seqxml","cor6_6.imgt","cor6_6.embl","cor6_6.fasta","cor6_6.gb"}
# for file in fileNames:
#     print(file, checkFileFmt(file))


# In[ ]:



