#!/bin/bash

if [$# -lt "5" ]; then
  echo "This script requires five arguments:"
  echo "  Reservation"
  echo "  time for the job"
  echo "  Allocation"
  echo "  File A path"
  echo "  File B path"
  exit -1
fi



reservation=$1
t=$2
allocation=$3
file_1_path=$4
file_2_path=$5


sbatch -J Compare_Sequence -o job.%j.out -p hadoop --reservation=$reservation -N 1 -n 1 -t $t -A $allocation<<ENDINPUT
#!/bin/bash

spark-submit --master yarn-client  --num-executors 4000 --executor-cores 3  --executor-memory 10G --driver-memory 50G compare_report.py $file_1_path $file_2_path

ENDINPUT
