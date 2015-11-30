#!/usr/bin/Rscript

library(rmr2)

args=commandArgs(trailingOnly = TRUE) 


bp =
  list(
    hadoop =
      list(
        D = paste("mapred.job.name=", args[[1]], sep=''),
        D = "mapreduce.map.memory.mb=8000",
        D = "mapreduce.reduce.memory.mb=8000",
        D = "mapreduce.map.java.opts=-Xmx8000M",
        D = "mapreduce.reduce.java.opts=-Xmx8000M",
	#D = "mapreduce.map.memory.mb=-1",
	#D = "mapreduce.reduce.memory.mb=-1",
        D = "mapreduce.tasktracker.map.tasks.maximum=1",
        D = "mapreduce.tasktracker.reduce.tasks.maximum=1",
	D = "mapreduce.input.fileinputformat.split.minsize=0",
	D = "mapreduce.jobtracker.maxtasks.perjob=-1",
	D = "mapreduce.reduce.input.limit=-1",
        D = paste("mapreduce.job.maps=", args[[2]], sep=''),
        D = paste("mapreduce.job.reduces=", args[[3]], sep=''),
	D = paste("R_LIBS=","/opt/apps/intel15/mvapich2_2_1/big-data-r/3.1.3/r-library:/opt/apps/intel15/mvapich2_2_1/Rstats/3.1.3/lib64/R/library",sep=''),
	D = paste("LD_LIBRARY_PATH=",Sys.getenv("TACC_MKL_LIB"),Sys.getenv("ICC_LIB"),sep=':')
        #D = paste("LD_LIBRARY_PATH=","/opt/apps/intel15/mvapich2_2_1/big-data-r/3.1.3/protobuf-2.5.0/lib:/opt/apps/intel15/mvapich2_2_1/Rstats/3.1.3/lib64/R/lib:/opt/apps/intel15/mvapich2/2.1rc1/lib:/opt/apps/intel15/mvapich2/2.1rc1/lib/shared:/opt/apps/intel/composer_xe_2015.1.133/mpirt/lib/intel64:/opt/apps/intel/composer_xe_2015.1.133/ipp/lib/intel64:/opt/apps/intel/composer_xe_2015.1.133/mkl/lib/intel64:/opt/apps/intel/composer_xe_2015.1.133/tbb/lib/intel64:/opt/apps/intel/composer_xe_2015.1.133/tbb/lib/intel64/gcc4.4:/opt/apps/intel/composer_xe_2015.1.133/compiler/lib/intel64",sep='')
                                ))


rmr.options(backend.parameters = bp);

rmr.options("backend.parameters")

wordcount = 
    function(input, output = args[[5]], pattern = " "){
        wc.map = function(., lines) {
#	    lines=gsub("[^[:alnum:]///' ]", "", lines)
            keyval(unlist(strsplit(
               x = lines,fixed=T,
               split = pattern)),1)}

 	wc.reduce = function(word, counts ) {
	    keyval(word, sum(counts))}
	      
	mapreduce(input = input,
            output = output,
            input.format = "text",
	    map = wc.map,
            reduce = wc.reduce,
            #backend.parameters = bp,
	    #combine = F #out of memory
	    #combine = T #out of memory
	    in.memory.combine = TRUE # this one works
	)}

wordcount(args[[4]])

#as.data.frame(from.dfs(wordcount(args[[3])))

