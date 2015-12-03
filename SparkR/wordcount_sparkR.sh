#!/bin/sh
export HADOOP_HOME=""
sparkR-submit  --master yarn --executor-memory 1g --num-executors 40 \
wordcount_sparkR.R yarn-client /user/rhuang/data/book.txt /user/rhuang/output/spark-1
export HADOOP_HOME=/usr/lib/hadoop
