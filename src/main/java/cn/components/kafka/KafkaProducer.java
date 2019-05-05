package cn.components.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

    private Producer<byte[], byte[]> producer;
    private String topic;

    KafkaProducer(String topic, ProducerConfig config) {
        this.topic = topic;
        producer = new Producer<>(config);
    }

    /**
     * @param messageByteArray 按topic发送消息
     */
    void send(byte[] messageByteArray) {
        KeyedMessage<byte[], byte[]> km = new KeyedMessage<>(topic, messageByteArray);
        producer.send(km);
    }

    /**
     * 按key分区发送
     *
     * @param key              key
     * @param messageByteArray message
     */
    public void sendKeyMessage(byte[] messageByteArray, byte[] key) {
        KeyedMessage<byte[], byte[]> km = new KeyedMessage<>(topic, key, null, messageByteArray);
        producer.send(km);
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
