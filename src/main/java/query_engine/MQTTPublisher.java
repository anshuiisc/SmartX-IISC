package query_engine;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import static java.lang.System.exit;

public class MQTTPublisher {

    public static void publishOutput(String Broker, String Topic, String Message) {
        MqttClient client;
        String clientID = "Spark";
        try {
            client = new MqttClient(Broker, clientID);

            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.isCleanSession();
            connOpts.setUserName("admin");
            connOpts.setPassword("password".toCharArray());
            client.connect(connOpts);
            MqttMessage message = new MqttMessage();
            message.setQos(0);

//            // Sending output in small chunks
//            String msg;
//            for (int i = 1; i <= 10000; i++) {
//                msg = "Message " + i;
//                System.out.println(msg);
//                message.setPayload(msg.getBytes());
//                client.publish(Topic, message);
//            }

            message.setPayload(Message.getBytes());
            client.publish(Topic, message);
            message.setPayload("ShutDown".getBytes());
            client.publish(Topic, message);
            client.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws MqttException {
        publishOutput("tcp://localhost:61613", "foo", "Message");
        exit(0);
    }
}
