package com.baoviet.kafka;

import java.util.*;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerReportingService {

	public static void main(String[] args) {
		int totalValid = 0;
		int totalSuspicious = 0;

		Properties props = new Properties();
//		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.2.196:9092,10.0.2.197:9093");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.2.195:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "banking-group");
//		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		try (Consumer<String, String> consumer = new KafkaConsumer<String, String>(props)) {
			consumer.subscribe(Arrays.asList("valid", "suspicious"));
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(1000);
				for (ConsumerRecord<String, String> record : records) {
					if (record.topic().equals("valid")) {
						totalValid += 1;
					} else if (record.topic().equals("suspicious")) {
						totalSuspicious += 1;
					}
					System.out.println("Total valid: " + totalValid + "\t\tTotal suspicious: " + totalSuspicious);
					System.out.println("Received topic \"" + record.topic() + "\": " + record.value());
				}
			}
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}
}
