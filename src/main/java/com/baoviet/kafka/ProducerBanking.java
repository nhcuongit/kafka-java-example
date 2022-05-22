package com.baoviet.kafka;

import java.util.*;
import java.sql.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerBanking {
	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.2.196:9092,10.0.2.197:9093");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-banking");
//		props.put(ProducerConfig.ACKS_CONFIG, "all");
//		props.put(ProducerConfig.RETRIES_CONFIG, 3);
//		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
//		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		try (Producer<String, String> producer = new KafkaProducer<String, String>(props)) {
			while (true) {
				System.out.print("Username: ");
				String username = scanner.next();
				System.out.print("Amount: ");
				int amount = scanner.nextInt();
				System.out.print("Location: ");
				String location = scanner.next();

				Hashtable<String, String> customer = getCustomer(username);
				if (customer != null) {
					String address = customer.get("address");
					String topic = location.toLowerCase().equals(address.toLowerCase()) ? "valid" : "suspicious";
					Timestamp transAt = new Timestamp(System.currentTimeMillis());

					// IGNORE: Insert transaction into banking database

					String value = username + " | " + transAt + " | " + amount + " | " + location;
					producer.send(new ProducerRecord<String, String>(topic, value));
					System.out.println("Sent " + username + " to topic " + topic);
				}

				System.out.print("Nhap y/Y de tiep tuc: ");
				String cmd = scanner.next();
				if (!cmd.toLowerCase().equals("y")) {
					break;
				}
			}
		} catch (Exception ex) {
			System.out.println(ex);
		} finally {
			scanner.close();
		}
	}

	private static Hashtable<String, String> getCustomer(String username) {
		Hashtable<String, String> customer = null;
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			String connString = "jdbc:mysql://10.0.2.195:3306/banking";
			String user = "kafka_user";
			String password = "Kafka@123";
			Connection conn = DriverManager.getConnection(connString, user, password);

			PreparedStatement stmt = conn.prepareStatement("SELECT * FROM customers WHERE username = ?");
			stmt.setString(1, username);
			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				customer = new Hashtable<String, String>();
				customer.put("username", res.getString(1));
				customer.put("address", res.getString(2));
			}
			conn.close();
		} catch (Exception ex) {
			customer = null;
			System.out.println(ex);
		}
		return customer;
	}
}
