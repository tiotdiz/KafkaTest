package com.qps.utils;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerUtil {

	private String brokerList;
	private KafkaConsumer<String, String> kafkaConsumer;

	public KafkaConsumerUtil(String brokerList, String topicName) {
		super();
		this.brokerList = brokerList;

		Properties properties = new Properties();
		properties
				.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerList);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "TestKafka");
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
		
		

		kafkaConsumer = new KafkaConsumer<String, String>(properties);
		kafkaConsumer.subscribe(Arrays.asList(topicName));
	}

	public void close() {
		this.kafkaConsumer.close();
	}

	public static void main(String[] args) {
		String brokerList = "sc-slave7:6667,sc-slave8:6667";
		String topic = "testKafka";

		KafkaConsumerUtil kafkaConsumerUtil = new KafkaConsumerUtil(brokerList,
				topic);

		for (int i = 0; i < 5; i++) {
			ConsumerRecords<String, String> records = kafkaConsumerUtil.kafkaConsumer
					.poll(1000);
			for(ConsumerRecord<String, String> record : records){
				System.out.println("topic = " + record.topic());
				System.out.println("offset = " + record.offset());
				System.out.println("value = " + record.value());
				System.out.println("------------------------------");
			}
		}
		
		kafkaConsumerUtil.close();

		System.out.println("Done!");
	}

}
