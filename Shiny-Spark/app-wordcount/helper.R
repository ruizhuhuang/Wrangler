split_kv=function(kv){
  s=lapply(kv, function(e)strsplit(e, split=" ",fixed=T))
  df = data.frame(matrix(unlist(s), ncol=2, byrow=T),
                  stringsAsFactors =F)
  df$X2=as.integer(df$X2)
  return(df)
}

getWordCountDF = function(input_file, num_executor){
  input_file = input_file
  num_executor= num_executor
  submit_cmd = paste("spark-submit --master yarn-client --num-executors ", num_executor, " wordcount.py ", input_file, " ", sep="")
  dat=data.frame()
  if (input_file!=""){
    kv=system(submit_cmd, intern = TRUE)
    #print(kv)
    dat=split_kv(kv)
  }
  return(dat)
}