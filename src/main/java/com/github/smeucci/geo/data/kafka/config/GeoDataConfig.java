package com.github.smeucci.geo.data.kafka.config;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
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
		HEMISPHERE_GEO_DATA_STATISTICS("hemisphere.geo.data.statistics"),
		COUNT_EVERY_QUARTER_HOUR_GEO_DATA("count.every.quarter.hour.geo.data");

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
		COUNT_BY_HEMISPHERE("count-by-hemisphere-store"),
		COUNT_LAST_30_MINUTES_BY_ID("count-last-30-minutes-by-id-store"),
		COUNT_EVERY_QUARTES_HOUR_BY_ID("count-every-quarter-hour-by-id-store"),
		COUNT_LAST_30_MINUTES("count-last-30-minutes-store"),
		COUNT_EVERY_QUARTES_HOUR("count-every-quarter-hour-store");

		private final String store;

		private Store(String store) {
			this.store = store;
		}

		public String storeName() {
			return this.store;
		}

	}

	public enum Operator {
		SOURCE_GEO_DATA("source-geo-data-operator"),
		COUNT_BY_HEMISPHERE("count-by-hemisphere-operator"),
		FILTER_NORTHERN("filter-northern-operator"),
		FILTER_SOUTHERN("filter-southern-operator"),
		FILTER_EQUATOR("filter-equator-operator"),
		SELECT_KEY_HEMISPHERE("select-key-hemisphere-operator"),
		BRANCH_BY_HEMISPHERE("branch-by-hemisphere-operator"),
		GROUP_BY_HEMISPHERE("group-by-hemisphere-operator"),
		GROUP_BY_GEO_DATA_ID("group-by-geo-data-id-operator"),
		GROUP_ALL("group-all-operator"),
		GROUP_BY_QUARTER_HOUR("group-by-quarter-hour-operator"),
		COUNT_LAST_30_MINUTES_BY_ID("count-last-30-minutes-by-id-operator"),
		COUNT_EVERY_QUARTES_HOUR_BY_ID("count-every-quarter-hour-by-id-operator"),
		COUNT_EVERY_QUARTES_HOUR("count-every-quarter-hour-operator"),
		REDUCE_LAST_30_MINUTES("reduce-last-30-minutes-operator"),
		SELECT_KEY_ALL_SAME("select-key-all-same-operator"),
		SELECT_KEY_QUARTER_HOUR("select-key-quarter-hour-operator"),
		TO_COUNT_EVERY_QUARTER_HOUR_STREAM("to-count-quarter-hour-stream-operator"),
		TO_COUNT_EVERY_QUARTER_HOUR_TOPIC("to-count-quarter-hour-topic-operator");

		private final String operator;

		private Operator(String operator) {
			this.operator = operator;
		}

		public String operatorName() {
			return this.operator;
		}

	}

	public static Properties producerProperties() {

		Properties properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GeoDataConfig.Server.KAFKA.address());
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return properties;

	}

	public static Properties consumerProperties() {

		Properties properties = new Properties();

		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, GeoDataConfig.Server.KAFKA.address());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-count-windowed-geo-data");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

		return properties;

	}

	public static Properties streamsProperties() {

		// create properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
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
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

		// disable caching so that data is processing as it arrives
		properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");

		return properties;

	}

}
