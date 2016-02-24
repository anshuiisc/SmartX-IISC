#!/usr/bin/env bash

#Starting Apollo
sh /home/smartx/solarBigDataStack/scripts/apollo.sh
#Starting HDFS
start-dfs.sh

#Starting HBase
start-dfs.sh

#starting Storm
#nice nohup storm dev-zookeeper & ### Not required if HBase is running
nice nohup storm nimbus &
nice nohup storm supervisor &
nice nohup storm ui &

pathStorm="/home/smartx/solarBigDataStack/smartx/water/water_v0.0.1/target"
cd ${pathStorm}
nice nohup storm jar dream-lab-0.0.1.jar  data_topology.WaterTopology WaterTopology &

pathNode="/home/smartx/solarBigDataStack/smartx/water/water_ui"
cd ${pathNode}
nice nohup node server.js &
