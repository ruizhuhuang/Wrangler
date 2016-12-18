node_1=`scontrol show hostnames $SLURM_NODELIST|head -n 1`
node_2=`scontrol show hostnames $SLURM_NODELIST|head -n 2|tail -n 1`
node_3=`scontrol show hostnames $SLURM_NODELIST|head -n 3|tail -n 1`
data_dir=$DATA/streaming/data/zk1
log_dir=$DATA/streaming/log/zk1
kk_log_dir=$DATA/streaming/log/kk1
zookeeper_connect=$node_1:2181,$node_2:2181,$node_3:2181
storm_local_dir=$DATA/streaming/log/storm1

echo $node_1
echo $node_2
echo $node_3
echo $data_dir
echo $log_dir

sed "s#data_dir#$data_dir#g; s#log_dir#$log_dir#g; s/server1/$node_1/g; s/server2/$node_2/g; s/server3/$node_3/g" zoo_default.cfg > zoo_1.cfg
sed "s/zk1/zk2/g" zoo_1.cfg >zoo_2.cfg
sed "s/zk1/zk3/g" zoo_1.cfg >zoo_3.cfg
echo 1 > $DATA/streaming/data/zk1/myid
echo 2 > $DATA/streaming/data/zk2/myid
echo 3 > $DATA/streaming/data/zk3/myid

sed "s/broker.id=0/broker.id=1/g; s#/tmp/kafka-logs#$kk_log_dir#g;s/localhost:2181/$zookeeper_connect/g" server.properties > server.1.properties
sed "s/broker.id=1/broker.id=2/g; s/kk1/kk2/g" server.1.properties > server.2.properties
sed "s/broker.id=1/broker.id=3/g; s/kk1/kk3/g" server.1.properties > server.3.properties

sed "s/server1/$node_2/g; s/server2/$node_3/g; s/host1/$node_1/g; s#storm_local_dir#$storm_local_dir#g" storm.yaml.default > storm.1.yaml
sed "s/#supervisor.slots.ports:/supervisor.slots.ports:\n    - 6700\n    - 6701\n    - 6702\n    - 6703/g; s/storm1/storm2/g" storm.1.yaml > storm.2.yaml
sed "s/storm2/storm3/g" storm.2.yaml > storm.3.yaml 




ssh $node_1 "cd $DATA;cp zoo_1.cfg zoo.cfg; export ZOOCFGDIR=$DATA; /usr/lib/zookeeper/bin/zkServer.sh start"

ssh $node_2 "cd $DATA;cp zoo_2.cfg zoo.cfg; export ZOOCFGDIR=$DATA; /usr/lib/zookeeper/bin/zkServer.sh start"

ssh $node_3 "cd $DATA;cp zoo_3.cfg zoo.cfg; export ZOOCFGDIR=$DATA; /usr/lib/zookeeper/bin/zkServer.sh start"

ssh $node_1 "cd $DATA;nohup kafka_2.11-0.10.1.0/bin/kafka-server-start.sh server.1.properties &>kk-1.out &"

ssh $node_2 "cd $DATA;nohup kafka_2.11-0.10.1.0/bin/kafka-server-start.sh server.2.properties &>kk-2.out &"

ssh $node_3 "cd $DATA;nohup kafka_2.11-0.10.1.0/bin/kafka-server-start.sh server.3.properties &>kk-3.out &"


ssh $node_1 "cd $DATA;nohup apache-storm-1.0.2/bin/storm --config /data/03076/rhuang/storm.1.yaml nimbus &> storm-1.out &"

ssh $node_2 "cd $DATA;nohup apache-storm-1.0.2/bin/storm --config /data/03076/rhuang/storm.2.yaml supervisor &> storm-2.out &"

ssh $node_3 "cd $DATA;nohup apache-storm-1.0.2/bin/storm --config /data/03076/rhuang/storm.3.yaml supervisor &> storm-3.out &"

ssh $node_1 "cd $DATA;nohup apache-storm-1.0.2/bin/storm ui &> storm-ui.out &" 

ssh $node_1 "cd $DATA;nohup ssh -f -g -N -R 58080:127.0.0.1:8080 login1 >> storm-ui.out 2>&1 &"



