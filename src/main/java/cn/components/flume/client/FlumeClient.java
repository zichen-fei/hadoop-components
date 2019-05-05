package cn.components.flume.client;

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FlumeClient {

    private static final Logger logger = LoggerFactory.getLogger(FlumeClient.class);

    public static void main(String[] args) throws Exception {
        int num = 1;
        for (int i = 0; i < num; i++) {
            new Client().start();
        }
        TimeUnit.MINUTES.sleep(10);
    }

    private static class Client extends Thread {

        @Override
        public void run() {

            RpcClient client = null;
            try {
                client = RpcClientFactory.getDefaultInstance("127.0.0.1", 41414);

                int num = 100;
                for (int i = 0; i < num; i++) {
                    try {
                        Map<String, String> headers = new HashMap<>(16);
                        String message = "SUBMIT 0 LS5A2DJX7JA000197 FORWARD {VID:LS5A2DJX7JA000197,VTYPE:0,TIME:20180912160918,FLAG:2,TYPE:2,RESULT:1,FORWARD_ID:19fd2a2e-c2f9-11e8-9869-90e2bae77ed8}";
                        message = message.replaceFirst("VID:LS5A2DJX7JA000197", "VID:" + "LS5A2DJX7JA00019" + i);
                        Event event = EventBuilder.withBody(message, Charset.forName("UTF-8"), headers);
                        logger.info(message);
                        client.append(event);
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } finally {
                if (client != null) {
                    client.close();
                }
            }
        }

    }
}
