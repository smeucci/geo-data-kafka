package com.github.smeucci.geo.data.kafka.config;

public class GeoDataConfig {

	public enum Server {
		ZOOKEEPER(""),
		KAFKA("");

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

}
