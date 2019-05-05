package cn.components.kafka;

import kafka.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProducerConfig {

	/**
	 * 利用基本配置 创建ProducerConfig
	 * @param brokerList 必须带端口号
	 * @return ProducerConfig
	 */
	public static ProducerConfig createProducerConfig(String brokerList){
		Properties props = new Properties();
		props.put("metadata.broker.list", brokerList);
		props.put("request.required.acks", "1");
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		props.put("request.timeout.ms", "10000");
		props.put("producer.type", "async");
		props.put("compression.codec", "lzo");
		return new ProducerConfig(props);
	}
	/**
	 * 完全自定义创建ProducerConfig
	 * @param kafkaprops kafka配置
	 * @return ProducerConfig
	 */
	static ProducerConfig createProducerConfig(Properties kafkaprops){
		return new ProducerConfig(kafkaprops);
	}
}
