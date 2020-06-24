package com.github.smeucci.geo.data.kafka.streams;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.streams.processing.FilterAndCountByHemisphere;

public class GeoDataStreams {

	private static final Logger log = LoggerFactory.getLogger(GeoDataStreams.class);

	public static void main(String[] args) {

		// create Producer properties
		Properties properties = GeoDataConfig.streamsProperties();

		// create kafka streams builder
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		// create stream from topic
		KStream<String, String> geoDataStream = streamsBuilder.stream(GeoDataConfig.Topic.SOURCE_GEO_DATA.topicName());

		// filter by hemisphere and count occurrences for each
		FilterAndCountByHemisphere.northern(geoDataStream);
		FilterAndCountByHemisphere.southern(geoDataStream);

		// build the topology
		Topology topology = streamsBuilder.build();

		// print topology
		log.info("{}", topology.describe());

		// create the kafka streams
		KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

		// start the stream application
		kafkaStreams.start();

		// add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

	}

}
