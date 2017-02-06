#!/bin/bash

echo "bash this script under $DATA using:"
echo "bash streaming_services/streaming_services_start.sh hadoop+Evaluating-Identifie+2060 10:00:00 Evaluating-Identifie"   

if [ $# -lt "3" ]; then
  echo "This script requires three arguments:"
  echo "  Reservation"
  echo "  time for the job"
  echo "  Allocation"
  exit -1
fi


reservation=$1
t=$2
allocation=$3


mkdir -p $DATA/streaming/data/zk1 $DATA/streaming/data/zk2 $DATA/streaming/data/zk3
mkdir -p $DATA/streaming/log/zk1 $DATA/streaming/log/zk2 $DATA/streaming/log/zk3
mkdir -p $DATA/streaming/log/kk1 $DATA/streaming/log/kk2 $DATA/streaming/log/kk3
mkdir -p $DATA/streaming/log/storm1 $DATA/streaming/log/storm2 $DATA/streaming/log/storm3


sbatch -J streaming_apps -o job.%j.out -p hadoop --reservation=$reservation -N 3 -n 3 -t $t -A $allocation << ENDINPUT
#!/bin/bash

/bin/bash stream_services.sh 
sleep 48h

ENDINPUT
