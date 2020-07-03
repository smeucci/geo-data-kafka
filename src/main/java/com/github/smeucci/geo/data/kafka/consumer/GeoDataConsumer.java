package com.github.smeucci.geo.data.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;

public class GeoDataConsumer {

	private static final Logger log = LoggerFactory.getLogger(GeoDataConsumer.class);

	public static void main(String[] args) throws Exception {

		// create Producer properties
		Properties properties = GeoDataConfig.consumerProperties();

		// create the producer
		KafkaConsumer<Windowed<String>, Long> consumer = new KafkaConsumer<Windowed<String>, Long>(properties);

		consumer.subscribe(Arrays.asList(GeoDataConfig.Topic.COUNT_EVERY_QUARTER_HOUR_GEO_DATA.topicName()));

		// add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

		while (true) {

			ConsumerRecords<Windowed<String>, Long> records = consumer.poll(Duration.ofSeconds(1));

			for (ConsumerRecord<Windowed<String>, Long> record : records) {

				log.info("key: {}, value: {}", record.key(), record.value());

			}

		}

	}

}
