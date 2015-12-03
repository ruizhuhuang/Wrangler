
# coding: utf-8

# In[3]:

import os
from os import listdir
from os.path import isfile, join
import filecmp


# In[9]:

dir_a="/Users/rhuang/Documents/Dropbox_1/TACC/EAGER/WGSOryza_CIAT_LSU_USDA_NCGR_SV"
dir_b="/Users/rhuang/Documents/Dropbox_1/TACC/EAGER/WGSOryza_CIAT_LSU_USDA_NCGR_SV_1"

def filesInPath(path):
    return [ f for f in listdir(path) if isfile(join(path,f)) ]   

list_a=filesInPath(dir_a)
list_b=filesInPath(dir_b)
name_a=[os.path.splitext(file)[0] for file in list_a ]
name_b=[os.path.splitext(file)[0] for file in list_b ]
dict_a=dict(zip(name_a,list_a))
dict_b=dict(zip(name_b,list_b))

set_a=set(dict_a.keys())
set_b=set(dict_b.keys())

print ('Files in set a not in set b is %s\n' % list(set_a - set_b))

print ('Files in set b not in set a is %s\n' % list(set_b - set_a))

# same name, compare file content with filecmp
intersection=set_a & set_b

for key in intersection:
    file_a=os.path.join(dir_a,  dict_a[key])
    file_b=os.path.join(dir_b,  dict_b[key])
    print(key, filecmp.cmp (file_a, file_b))
    


# In[ ]:



