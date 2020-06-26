package com.github.smeucci.geo.data.kafka.config;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

public final class GeoDataConfig {

	private static final String STREAMS_APP_ID = "kafka-streams-app-gdk";

	public enum Server {
		ZOOKEEPER("localhost:2181"),
		KAFKA("localhost:9092");

		private final String address;

		private Server(String address) {
			this.address = address;
		}

		public String address() {
			return this.address;
		}
	}

	public enum Topic {
		SOURCE_GEO_DATA("source.geo.data"),
		NORTHERN_HEMISPHERE_GEO_DATA("northern.hemisphere.geo.data"),
		SOUTHERN_HEMISPHERE_GEO_DATA("southern.hemisphere.geo.data"),
		HEMISPHERE_GEO_DATA_STATISTICS("hemisphere.geo.data.statistics");

		private final String topicName;

		private Topic(String topicName) {
			this.topicName = topicName;
		}

		public String topicName() {
			return topicName;
		}
	}

	public enum Key {
		NORTHERN_HEMISPHERE("northern_hemisphere"),
		SOUTHERN_HEMISPHERE("southern_hemisphere");

		private final String key;

		private Key(String key) {
			this.key = key;
		}

		public String keyValue() {
			return this.key;
		}
	}

	public enum Store {
		COUNT_BY_HEMISPHERE("count-by-hemisphere-store");

		private final String store;

		private Store(String store) {
			this.store = store;
		}

		public String storeName() {
			return this.store;
		}

	}

	public enum Processor {
		COUNT_BY_HEMISPHRE("count-by-hemisphere-processor"),
		FILTER_NORTHERN("filter-northern-processor"),
		FILTER_SOUTHERN("filter-southern-processor"),
		FILTER_EQUATOR("filter-equator-processor"),
		SELECT_KEY_HEMISPHERE("select-key-hemisphere-processor");

		private final String processor;

		private Processor(String processor) {
			this.processor = processor;
		}

		public String processorName() {
			return this.processor;
		}

	}

	public static Properties producerProperties() {

		Properties properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GeoDataConfig.Server.KAFKA.address());
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return properties;

	}

	public static Properties streamsProperties() {

		// create properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

		// disable caching so that data is processing as it arrives
		properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Server.KAFKA.address());
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, STREAMS_APP_ID);

		return properties;

	}

	public static Properties testStreamsProperties() {

		// create properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

		// disable caching so that data is processing as it arrives
		properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");

		return properties;

	}

}
