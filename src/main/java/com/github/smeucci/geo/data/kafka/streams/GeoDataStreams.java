package com.github.smeucci.geo.data.kafka.streams;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.config.GeoDataConfig.Topic;
import com.github.smeucci.geo.data.kafka.streams.dsl.CountByHemisphere;
import com.github.smeucci.geo.data.kafka.streams.dsl.FilterByHemisphere;
import com.github.smeucci.geo.data.kafka.topology.GeoDataTopology;

public class GeoDataStreams {

	private static final Logger log = LoggerFactory.getLogger(GeoDataStreams.class);

	public static void main(String[] args) {

		// create Producer properties
		Properties properties = GeoDataConfig.streamsProperties();

		// build the topology
		Topology topology = new GeoDataTopology(Topic.SOURCE_GEO_DATA) //
				.addOperator(FilterByHemisphere::northern) //
				.addOperator(FilterByHemisphere::southern) //
				.addOperator(CountByHemisphere::count) //
				.build();

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
