package cn.components.kafka;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;

import cn.components.utils.Utils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * nohup java -jar flume-serializer 1 1 1 /message.txt test async localhost:9092
 */
public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    private Producer() {
        before("", "sync", "");
    }

    public static void main(String[] args) {
        int threadNum = Integer.valueOf(args[2]);
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("producer-pool-%d").build();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(Integer.valueOf(args[0]), Integer.valueOf(args[1]),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());

        for (int i = 0; i < threadNum; i++) {
            threadPoolExecutor.execute(new SendData(args[3], args[4], args[5], args[6]));
        }
    }

    private static KafkaProducer before(String topic, String type, String brokerAddress) {
        Properties props = new Properties();
        //只用于获取metadata，可以配置为kafka集群的自己
        props.put("metadata.broker.list", brokerAddress);
        props.put("request.required.acks", "1");
        //async  异步提交
        props.put("producer.type", type);
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        ProducerConfig config = KafkaProducerConfig.createProducerConfig(props);
        //设置topic
        return new KafkaProducer(topic, config);
    }


    private static class SendData implements Runnable {

        private List<String> datas;

        private String topic;

        private String type;

        private String brokerAddress;

        SendData(String fileName, String topic, String type, String brokerAddress) {
            this.datas = Utils.readFile(Utils.base_path + fileName);
            this.topic = topic;
            this.type = type;
            this.brokerAddress = brokerAddress;
        }

        @Override
        public void run() {
            KafkaProducer producer = before(topic, type, brokerAddress);
            int count = 0;
            long startTime = System.currentTimeMillis();
            while (true) {
                for (String data : datas) {
                    producer.sendKeyMessage(data.getBytes(), Bytes.toBytes(Math.abs(UUID.randomUUID().toString().hashCode()) % 10));
                    logger.info(data);
                    count++;
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (count % 1000 == 0) {
                        long time = System.currentTimeMillis();
                        logger.info("===> Thread: {}, Producer produce: {} item, used:{} ms", Thread.currentThread().getName(),
                                String.valueOf(count),  String.valueOf(time - startTime));
                    }
                }
            }
        }
    }

}
