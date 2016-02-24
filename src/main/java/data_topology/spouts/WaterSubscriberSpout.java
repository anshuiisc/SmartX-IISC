package data_topology.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import config.WaterConfig;
import org.eclipse.paho.client.mqttv3.*;

import java.util.LinkedList;
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
public class WaterSubscriberSpout implements MqttCallback, IRichSpout {
    MqttClient client;
    SpoutOutputCollector _collector;
    LinkedList<String> messages = new LinkedList<String>();

    public void messageArrived(String topic, MqttMessage message)
            throws Exception {
        messages.add(message.toString());
//        System.out.println(message.toString());
    }

    public void connectionLost(Throwable cause) {
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        _collector = collector;

        try {
            client = new MqttClient(WaterConfig.APOLLO_URL, WaterConfig.APOLLO_CLIENT);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setUserName(WaterConfig.APOLLO_USER);
            connOpts.setPassword(WaterConfig.APOLLO_PASSWORD.toCharArray());
            client.connect(connOpts);
            client.setCallback(this);
            //client.subscribe(WaterConfig.DATA_DESTINATION);
            client.subscribe(WaterConfig.SENSOR_DATA_DESTINATION);
            client.subscribe(WaterConfig.NETWORK_DATA_DESTINATION);

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    public void deactivate() {
    }

    public void nextTuple() {
        if (!messages.isEmpty()) {
            _collector.emit(new Values(messages.poll()));
        }
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("WaterData"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
