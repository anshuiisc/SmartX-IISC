package query_engine;

import config.WaterConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Replace this line with a description of what this class does.
 * Feel free to be verbose and descriptive for key classes.
 *
 * @author Arun Verma [mailto:arunverma100@gmail.com]
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
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
public class ReadHBase {
    // HBase Config
    private static Configuration conf = HBaseConfiguration.create();
    public static final String ZK_QUORUM;
    public static final String ZK_CLIENT_PORT;

    // Initializing the Static variables
    static {
        ZK_QUORUM = "hbase.zookeeper.quorum";
        ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    }

    // Return a list for Spark RDD from HBase scanned result
    public static ArrayList<String> buildList(ResultScanner resultScanner) {
        Iterator<Result> iterator = resultScanner.iterator();
        StringBuilder nextValue = new StringBuilder("");
        // sensorCount for checking number of sensors
        int columnCount = 1;
        ArrayList<String> list = new ArrayList<String>();
        while(iterator.hasNext()) {
            Result result = iterator.next();
            Cell[] rawCells = result.rawCells();

            for (Cell cell : rawCells) {
                if (columnCount == 1) {
                    nextValue.append(Bytes.toString(CellUtil.cloneRow(cell)) + ","
                            + Bytes.toString(CellUtil.cloneValue(cell)));
                } else {
                    nextValue.append("," + Bytes.toString(CellUtil.cloneValue(cell)));
                }
                columnCount++;
            }
            list.add(nextValue.toString());
            nextValue.setLength(0);
            columnCount = 1;
        }
//        System.out.println(list);
        return list;
    }

    // Scanning HBase table for retrieving sensors data
    public static ArrayList<String> getRangeData(
            ArrayList columnList, String startRow, String stopRow)
            throws IOException {

        // Creating HBase Connection
        String tableName = WaterConfig.HBASE_DATA_TABLE;
        conf.set(ZK_QUORUM, WaterConfig.HBASE_ZK_QUORUM_IP);
        conf.set(ZK_CLIENT_PORT, WaterConfig.HBASE_ZK_CLIENT_PORT);
        Connection connection = ConnectionFactory.createConnection(conf);
        byte[] TABLE_NAME = Bytes.toBytes(WaterConfig.HBASE_DATA_TABLE);
        Table table = connection.getTable(TableName.valueOf(tableName));
        String family, qualifier;
        Scan scan = new Scan();

        int sensorCount = 0;
        // Adding columns with family and adding family:qualifier to list
        for (Object aColumnList : columnList) {
            family = aColumnList.toString().split(":")[0];
            qualifier = aColumnList.toString().split(":")[1];

            // Adding columns with family in HBase Scan
            if (StringUtils.isNotBlank(family) && StringUtils.isNotEmpty(qualifier)) {
                scan.addColumn(family.getBytes(), qualifier.getBytes());
            }
        }
        if(StringUtils.isNotEmpty(startRow)){
            scan.setStartRow(startRow.getBytes());
        }
        if(StringUtils.isNotEmpty(stopRow)){
            scan.setStopRow(stopRow.getBytes());
        }
        AggregationClient aggregationClient = new AggregationClient(conf);

        // Scanning HBase for given sensor query
        ResultScanner resultScanner = table.getScanner(scan);
        ArrayList<String> list = buildList(resultScanner);
        return list;
    }
}
