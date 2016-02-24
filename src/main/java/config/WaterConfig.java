package config;

/**
 * Replace this line with a description of what this class does.
 * Feel free to be verbose and descriptive for key classes.
 *
 * @author Arun Verma [mailto:arunverma100@gmail.com]
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
 *
 * Copyright 2015 DREAM:Lab, Indian Institute of Science, Bangalore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class WaterConfig {

    // Apollo Broker Setting
    public static final String APOLLO_USER = "admin";
    public static final String APOLLO_PASSWORD = "password";
    public static final String APOLLO_URL = "tcp://smartx:1883";
    public static final String APOLLO_CLIENT = "dataTopology";
    //public static final String DATA_DESTINATION = "iisc/smartx/water/#";
    public static final String SENSOR_DATA_DESTINATION = "iisc/smartx/sensor/#";    
    public static final String NETWORK_DATA_DESTINATION = "iisc/smartx/network/#";    

    // HBase Table
    public static final String HBASE_ZK_QUORUM_IP = "smartx";
    public static final String HBASE_ZK_CLIENT_PORT = "2181";
    public static final String HBASE_DATA_TABLE = "waterData";

    // HDFS setting
    public static final String HDFS_URL = "hdfs://smartx:9000";
    public static final String HDFS_PATH = "/waterData/";
    public static final String HDFS_FILE_NAME = "water";
    public static final String HDFS_FILE_EXTENSION = ".txt";
    public static final int TUPLE_IN_BUFFER = 10;

}
