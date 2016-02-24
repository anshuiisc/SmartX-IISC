package data_topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import config.WaterConfig;
import data_topology.bolts.StormHBaseBolt;
import data_topology.spouts.WaterSubscriberSpout;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

/**
 * Replace this line with a description of what this class does.
 * Feel free to be verbose and descriptive for key classes.
 *
 * @author Arun Verma [mailto:arunverma100@gmail.com]
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
 * <p/>
 * Copyright 2015 DREAM:Lab, Indian Institute of Science, Bangalore
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class WaterTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        Config stormConf = new Config();
        stormConf.setDebug(true);
        stormConf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        TopologyBuilder builder = new TopologyBuilder();


        // Building Topology
        builder.setSpout("water-subscriber-spout", new WaterSubscriberSpout(), 1);
        builder.setBolt("storm-hbase-bolt", new StormHBaseBolt(), 1)
                .shuffleGrouping("water-subscriber-spout");


        // Use pipe as record boundary
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(",");

        // Synchronize data buffer with the filesystem every n tuple(n define in WaterConfig file)
        SyncPolicy syncPolicy = new CountSyncPolicy(WaterConfig.TUPLE_IN_BUFFER);

        // Rotate data files when they reach five MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(
                1.0f, FileSizeRotationPolicy.Units.GB);

        // Customize file name
        FileNameFormat fileNameFormat = new CustomName()
                .withPath(WaterConfig.HDFS_PATH)
                .withPrefix(WaterConfig.HDFS_FILE_NAME)
                .withExtension(WaterConfig.HDFS_FILE_EXTENSION);

        // HdfsBolt
        builder.setBolt("storm-hdfs-bolt", new HdfsBolt()
                        .withFsUrl(WaterConfig.HDFS_URL)
                        .withFileNameFormat(fileNameFormat)
                        .withRecordFormat(format)
                        .withRotationPolicy(rotationPolicy)
                        .withSyncPolicy(syncPolicy)
        ).shuffleGrouping("water-subscriber-spout");

        // Running Topology
        if (args != null && args.length > 0) {
            stormConf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], stormConf, builder.createTopology());
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Water_Topology", stormConf, builder.createTopology());
        }
    }
}
