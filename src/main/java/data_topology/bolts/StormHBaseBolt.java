package data_topology.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import config.WaterConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

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


public class StormHBaseBolt implements IRichBolt {

    public static Configuration config = HBaseConfiguration.create();
    public static final String ZK_QUORUM  = "hbase.zookeeper.quorum";
    public static final String ZK_CLIENT_PORT  = "hbase.zookeeper.property.clientPort";
    private static Connection connection;
    Table _table;
    String _column;
    String _str[];
    OutputCollector _collector;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        try {
            config.set(ZK_QUORUM, WaterConfig.HBASE_ZK_QUORUM_IP);
            config.set(ZK_CLIENT_PORT, WaterConfig.HBASE_ZK_CLIENT_PORT);
            connection = ConnectionFactory.createConnection(config);
            _table = connection.getTable(TableName.valueOf(WaterConfig.HBASE_DATA_TABLE));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple) {
        _str = tuple.getString(0).split(",");
        try {
            Put put = new Put(Bytes.toBytes(_str[0]));
            for (int i = 2; i < _str.length; i++) {
                _column = _str[1].split(":")[1] + "-" + _str[i].split(":")[0];
                put.addColumn(Bytes.toBytes(_str[1].split(":")[0]),
                        Bytes.toBytes(_column),
                        Bytes.toBytes(_str[i].split(":")[1]));
            }
            _table.put(put);

        } catch (IOException i) {
            i.printStackTrace();
//            System.out.println("Data fail");
        }
        catch (NullPointerException n){
            n.printStackTrace();
//            System.out.println("NullException for Id: " + _str[0]);
        }
        _collector.ack(tuple);

    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data"));
    }

    public Map getComponentConfiguration() {
        return null;
    }
}
