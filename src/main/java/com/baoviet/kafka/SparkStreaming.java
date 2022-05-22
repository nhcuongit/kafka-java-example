package com.baoviet.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class SparkStreaming {

	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("WordCount");
		sparkConf.set("spark.serializer", KryoSerializer.class.getName());
		sparkConf.setMaster("local");

		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(3));

		Map<String, Object> kafkaParams = new HashMap<>();
//		kafkaParams.put("bootstrap.servers", "10.0.2.196:9092,10.0.2.197:9093");
		kafkaParams.put("bootstrap.servers", "10.0.2.195:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "kafka-spark-streaming");
//		kafkaParams.put("auto.offset.reset", "latest");
//		kafkaParams.put("enable.auto.commit", false);
		Collection<String> topics = Arrays.asList("valid", "suspicious");

		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
				streamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		JavaPairDStream<String, Integer> wordCounts = messages
				.mapToPair(record -> new Tuple2<>(record.key(), record.value()))
				.map(tuple2 -> tuple2._2())
				.flatMap(x -> Arrays.asList(x.split("\\s+")).iterator())
				.mapToPair(s -> new Tuple2<>(s, 1))
				.reduceByKey((i1, i2) -> i1 + i2);
		wordCounts.print();

		streamingContext.start();
		streamingContext.awaitTermination();
	}
}
