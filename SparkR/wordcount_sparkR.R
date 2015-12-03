library(SparkR)

#Sys.setenv(SPARK_MEM="11000m")


time0=proc.time()

args=commandArgs(trailingOnly = TRUE)

if(length(args) != 3){
  cat("USE:sparkR-submit  --master yarn --executor-memory 1g --num-executors 40  wordcount_sparkR.R yarn-client <input> <output> ")
  q("no")
}

# Initialize Spark context
sc=sparkR.init(args[[1]],"RWordCount")

lines = textFile(sc, args[[2]],minSplits=6000)

words = flatMap(lines, 
                function(line){
                  #as.character(splitIntoWords(gsub("\\s+"," ", trimWhiteSpace(line))))
                  strsplit(line, fixed=TRUE, split=" ")[[1]]
})


wordCount = lapply(words, 
                   function(word){
                      list(word, 1L)
})


count = reduceByKey(wordCount, "+",4000L)
saveAsTextFile(count,args[[3]])
#output = collect(count)
proc.time()-time0

