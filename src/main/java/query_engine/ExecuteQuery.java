package query_engine;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;

import static org.apache.spark.sql.functions.*;

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
public class ExecuteQuery {

    public static long getEpochTime(String rowDate) {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss a");
        //Assigning current time zone or sensor timezone
        sdf.setTimeZone(TimeZone.getTimeZone("UTC + 5:30"));
        Date date = null;
        try {
            date = sdf.parse(rowDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return (date.getTime()/1000);
    }

    /**
     * args[0] -> sensorName
     * args[1] -> startTime
     * args[2] -> endTime
     * args[3] -> bucketLengthSec
     * args[4] -> parameter e.g. max, min etc.
     * args[5] -> connectionType e.g. Sync/Async
     * args[6] -> brokerUrl
     * args[7] -> topic
     * arg[8] -> protocol e.g. MQTT, AMQP
     * args[9] -> queryType e.g. data, tele aggregator
     */
    public static void main (String [] args) throws IOException {

//        String sensorIds="tele:sensor1-tele2";
////        String sensorIds="data:sensor1-level";
//
//        final String startRow = "1447352139";
//        String stopRow = "1447352473";
//        final long bucketLengthSec = 12;
//        String parameter = "Max";
//        String connectionType = "Sync";

        // Query Data
        String sensorIds = args[0];
        String startRow = args[1];
        String stopRow = args[2];
        Long bucketLengthSec = Long.parseLong(args[3]);
        String parameter = args[4];
        String connectionType = args[5];

        // Broker Data
        String brokerUrl = args[6];
        String topic = args[7];
        String protocol = args[8];
//        String queryType = args[9];

        if( connectionType.equals("Async")) {
            System.out.print("True");
        }

        ArrayList columnList =  new ArrayList<String>(Arrays.asList(sensorIds.split(",")));

        // For running Spark on LOCAL MACHINE
        // SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("SensorData");

        // For running Spark on SPARK CLUSTER
        SparkConf conf = new SparkConf().setAppName("SensorData");

	JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList list;
        list = ReadHBase.getRangeData(columnList, startRow, stopRow);
//        System.out.println(list);
        // Convert into JavaRDD
        JavaRDD<String> solarRDD = sc.parallelize(list);
        JavaRDD<DataFormat> sensorData = solarRDD.map(
                new Function<String, DataFormat>() {
                    public DataFormat call(String line) throws Exception {
                        String[] parts = line.split(",");

                        DataFormat dataFormat = new DataFormat();
                        dataFormat.setTimeStamp(Long.parseLong(parts[0]));
                        dataFormat.setValue(Float.parseFloat(parts[1]));

                        return dataFormat;
                    }
                });


        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        DataFrame df = sqlContext.createDataFrame(sensorData, DataFormat.class);
        df.registerTempTable("dataFormat");

        long start = Long.parseLong(startRow);
        long end = Long.parseLong(stopRow) - 1;
        long nextStart = start + bucketLengthSec;

        JSONArray appendResults = new JSONArray();
        JSONObject stepResults;
        Row row;

        if (parameter.equals("Max")) {
            while (start <= end) {
                row = df.filter(df.col("timeStamp")
                        .between(start, nextStart))
                        .agg(max(df.col("timeStamp")), max(df.col("value")))
                        .first();
                if (row.get(0) != null) {
                    stepResults = new JSONObject();
                    stepResults.put("x", Long.parseLong(row.get(0).toString()));
                    stepResults.put("y", row.get(1));
                    appendResults.add(stepResults);
                }
                start = nextStart;
                nextStart = start + bucketLengthSec;
            }
        }

        else if (parameter.equals("Min")) {
            while (start <= end) {
                row = df.filter(df.col("timeStamp")
                        .between(start, nextStart))
                        .agg(max(df.col("timeStamp")), min(df.col("value")))
                        .first();
                if (row.get(0) != null) {
                    stepResults = new JSONObject();
                    stepResults.put("x", Long.parseLong(row.get(0).toString()));
                    stepResults.put("y", row.get(1));
                    appendResults.add(stepResults);
                }
                start = nextStart;
                nextStart = start + bucketLengthSec;
            }
        }

        else if (parameter.equals("Mean")) {
            while (start <= end) {
                row = df.filter(df.col("timeStamp")
                        .between(start, nextStart))
                        .agg(max(df.col("timeStamp")), mean(df.col("value")))
                        .first();
                if (row.get(0) != null) {
                    stepResults = new JSONObject();
                    stepResults.put("x", Long.parseLong(row.get(0).toString()));
                    stepResults.put("y", row.get(1));
                    appendResults.add(stepResults);
                }
                start = nextStart;
                nextStart = start + bucketLengthSec;
            }
        }

        else if (parameter.equals("Range")) {
            while (start <= end) {
                row = df.filter(df.col("timeStamp")
                        .between(start, nextStart))
                        .agg(max(df.col("timeStamp")), min(df.col("value")), max(df.col("value")))
                        .first();
                if (row.get(0) != null) {
                    stepResults = new JSONObject();
                    stepResults.put("x", Long.parseLong(row.get(0).toString()));
                    stepResults.put("y", Float.parseFloat(row.get(2).toString()) - Float.parseFloat(row.get(1).toString()));
                    appendResults.add(stepResults);
                }
                start = nextStart;
                nextStart = start + bucketLengthSec;
            }
        }

        else {
            stepResults = new JSONObject();
            stepResults.put("x", "null");
            stepResults.put("y", "null");
            appendResults.add(stepResults);
        }

        JSONObject finalResults = new JSONObject();
        finalResults.put("result", appendResults);

        if( connectionType.equals("Async")) {
            MQTTPublisher.publishOutput(brokerUrl, topic, finalResults.toString());
        }
        else {
            System.out.println(finalResults);
        }
        System.exit(0);

//        System.out.println(finalResults.toJSONString());
//        System.out.println(finalResults.toString());
//        System.out.println(parameter + " " + finalResults);
    }
}
