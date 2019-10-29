package com.qps.utils;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerUtil {

	private String brokerList;
	private KafkaProducer<String, String> kafkaProducer;

	public KafkaProducerUtil(String brokerList) {
		super();
		this.brokerList = brokerList;

		Properties properties = new Properties();
		properties
				.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerList);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());

		kafkaProducer = new KafkaProducer<String, String>(properties);
	}
	
	public void close() {
		this.kafkaProducer.close();
	}

	public static void main(String[] args) {
		String brokerList = "sc-slave7:6667,sc-slave8:6667";
		String topic = "testKafka";
		
		String message = "你好，heelowrold";
		KafkaProducerUtil kafkaProducerUtil = new KafkaProducerUtil(brokerList);
		kafkaProducerUtil.kafkaProducer.send(new ProducerRecord<String, String>(topic, message));
		
		kafkaProducerUtil.close();
		
		System.out.println("Done!");
	}

}
