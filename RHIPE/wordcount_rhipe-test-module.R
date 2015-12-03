#!/usr/bin/env Rscript
Sys.getenv("LD_LIBRARY_PATH")
library(Rhipe)
rhinit()

rhoptions(runner = '/opt/apps/intel15/mvapich2_2_1/Rstats/3.1.3/lib64/R/bin/R CMD /opt/apps/intel15/mvapich2_2_1/big-data-r/3.1.3/r-library/Rhipe/bin/RhipeMapReduce --slave --silent --vanilla') #--max-ppsize=100000 --max-nsize=1G

#rhoptions()



#rhoptions(runner ='sh /home/03076/rhuang/RHIPE/RhipeMapReduce.sh')

#rhoptions(runner ='/home/03076/rhuang/R/lib64/R/bin/R CMD /home/03076/rhuang/R/lib64/R/library/Rhipe/bin/RhipeMapReduce --slave --silent --vanilla')

#input.file.local <- 'sample.txt'
input.user.dir <- paste('/user/',Sys.getenv('USER'),sep='')

args=commandArgs(trailingOnly = TRUE)

rhclean()

time0=proc.time()

if(length(args) < 2){
  input.file.name<- 'book100.txt'
 # input.file.name <- 'enwiki-latest-pages-meta-current1.xml'
  input.testnum<-1
  num_reducers<-0
  split_size_mb<-10
  combiner<-T
}else{
  input.testnum <- args[[1]] 
  input.file.name <- args[[2]]
  num_reducers<- args[[4]]
  split_size_mb<- as.integer(args[[3]])
  if(args[[5]]=="true"){
	combiner<-T
  }else{
	combiner<-F
  }
}

input.file.hdfs <- paste(input.user.dir,'/data/',input.file.name,sep='')
output.dir.hdfs <- paste(input.user.dir,'/output/rhipe_',format(Sys.time(), "%H%M%OS"),input.file.name,"M",split_size_mb,"R",num_reducers,sep='')
output.file.local <- paste('rhipe_',format(Sys.time(),"%H%M%OS"),input.file.name,split_size_mb,'M',num_reducers,'R',sep='')

mapper <- expression( {
    # 'map.values' is a list containing each line of the input file
    lines <- gsub('(^\\s+|\\s+$)', '', map.values)
    keys <- unlist(strsplit(lines, split='\\s+'))
    value <- 1
    lapply(keys, FUN=rhcollect, value=value)
} )

reducer <- expression(
    # 'reduce.key' is equivalent to this_key and set by Rhipe
    # 'reduce.values' is a list of values corresponding to this_key
    # 'pre' is executed before we process a new reduce.key
    # 'reduce' is executed for 'reduce.values'
    # 'post' is executed once all reduce.values are processed
    pre = {
        running_total <- 0
    },
    reduce = {
        running_total <- sum(running_total, unlist(reduce.values))
    },
    post = {
        rhcollect(reduce.key, running_total)
    }
)


# equivalent to hadoop dfs -copyFromLocal
#rhput(input.file.local, input.file.hdfs)

 mapred = list(
#            mapred.task.timeout=1
	     mapred.max.split.size=as.integer(1024*1024*split_size_mb)
            ,mapreduce.job.reduces=num_reducers  #CDH3,4,
	    ,LD_LIBRARY_PATH=paste("/opt/apps/intel15/mvapich2_2_1/big-data-r/3.1.3/protobuf-2.5.0/lib",Sys.getenv("TACC_MKL_LIB"),Sys.getenv("ICC_LIB"),sep=':') #Sys.getenv("LD_LIBRARY_PATH")
        )


rhipe.results <- rhwatch(
                        map=mapper, reduce=reducer,
                        input=rhfmt(input.file.hdfs, type="text"),
                        output=rhfmt(output.dir.hdfs,type="text"),
                        jobname=paste("rhipe-",split_size_mb,num_reducers,combiner,input.file.name,sep="-"),
                        mapred=mapred,
			combiner=combiner,readback=T)

                        # mapred=list(paste("mapreduce.job.maps",split_size_mb,sep='='),
            #, mapred.job.reuse.jvm.num.tasks=-1
            #, mapreduce.job.jvm.numtasks=-1
                        #   paste("mapreduce.job.reduces",num_reducers,sep='=')))
# the mapred=... parameter is optional in rhwatch() above

# results on HDFS are in Rhipe object binary format, NOT ASCII, and must be
# read using rhread().  results becomes a list of two-item lists (key,val)

###outputfiles<-rhls(paste(output.dir.hdfs,"/part-*",sep=""))

###for( a in outputfiles$file){
###  results <- rhread(a)
###  cat(a,"\n")

###  # the data.frame() below converts list of (key,val) to a list of keys and
###  # a list of vals, then dumps these into a file with tab delimitation
###  write.table( data.frame(words=unlist(lapply(X=results,FUN="[[",1)), 
###                         count=unlist(lapply(X=results,FUN="[[",2))), 
###              file=output.file.local,
###              quote=FALSE,
###	      append=TRUE, 
###              row.names=FALSE, 
###              col.names=FALSE,
###              sep="\t"
###              )
###}

proc.time()-time0
