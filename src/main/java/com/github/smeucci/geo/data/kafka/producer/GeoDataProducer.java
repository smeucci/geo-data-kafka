package com.github.smeucci.geo.data.kafka.producer;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.converter.GeoDataConverter;
import com.github.smeucci.geo.data.kafka.record.GeoData;

public class GeoDataProducer {

	private static final Logger log = LoggerFactory.getLogger(GeoDataProducer.class);

	private static final GeoDataConverter converter = new GeoDataConverter();

	public static void main(String[] args) throws Exception {

		// create Producer properties
		Properties properties = GeoDataConfig.producerProperties();

		// create the producer
		KafkaProducer<Long, String> producer = new KafkaProducer<Long, String>(properties);

		// create scheduler
		final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
		executorService.scheduleAtFixedRate(() -> produce(producer), 0, 60, TimeUnit.SECONDS);

		// add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			terminate(producer);
		}));

	}

	private static void produce(Producer<Long, String> producer) {

		GeoData geoData = GeoData.generate();

		log.info("{}", geoData);

		String topic = GeoDataConfig.Topic.SOURCE_GEO_DATA.topicName();
		Long key = geoData.id();
		String value = converter.toJson(geoData);

		// create a producer record
		ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(topic, key, value);

		producer.send(record);

	}

	private static void terminate(Producer<Long, String> producer) {
		producer.flush();
		producer.close();
	}

}
