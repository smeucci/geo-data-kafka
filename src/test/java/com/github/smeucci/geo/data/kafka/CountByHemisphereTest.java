package com.github.smeucci.geo.data.kafka;

import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.converter.GeoDataConverter;
import com.github.smeucci.geo.data.kafka.record.GeoData;
import com.github.smeucci.geo.data.kafka.streams.processing.CountByHemisphere;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CountByHemisphereTest {

	private static final Logger log = LoggerFactory.getLogger(CountByHemisphereTest.class);

	private static final GeoDataConverter converter = new GeoDataConverter();

	private TopologyTestDriver testDriver;
	private TestInputTopic<String, String> inputTopic;
	private TestOutputTopic<String, Long> outputTopic;

	@BeforeEach
	private void beforEach() {

		log.info("============================================================");
		log.info("==================== S T A R T  T E S T ====================");
		log.info("============================================================");

		Properties properties = GeoDataConfig.testStreamsProperties();

		// create kafka streams builder
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		// create stream from topic
		KStream<String, String> geoDataStream = streamsBuilder.stream(GeoDataConfig.Topic.SOURCE_GEO_DATA.topicName());

		// filter by hemisphere and count occurrences for each
		CountByHemisphere.count(geoDataStream);

		// build the topology
		Topology topology = streamsBuilder.build();

		// setup test driver
		testDriver = new TopologyTestDriver(topology, properties);

		// create test input topic
		inputTopic = testDriver.createInputTopic(GeoDataConfig.Topic.SOURCE_GEO_DATA.topicName(),
				new StringSerializer(), new StringSerializer());

		// create test output
		outputTopic = testDriver.createOutputTopic(GeoDataConfig.Topic.HEMISPHERE_GEO_DATA_STATISTICS.topicName(),
				new StringDeserializer(), new LongDeserializer());

	}

	@AfterEach
	public void afterEach() {

		if (this.testDriver != null) {
			this.testDriver.close();
		}

		log.info("============================================================");
		log.info("============================================================");

	}

	@Test
	@Order(1)
	@DisplayName("Test Count By Hemisphere")
	public void testCountByHemisphere() {

		log.info("==> testCountByHemisphere...");

		int numNorthern = 15;
		int numSouthern = 10;

		log.info("Generating {} northern hemisphere geo data...", numNorthern);

		Stream<GeoData> northernStream = IntStream.range(0, numNorthern).mapToObj(i -> GeoData.generateNorthen());

		log.info("Generating {} southern hemisphere geo data...", numSouthern);

		Stream<GeoData> southernStream = IntStream.range(0, numSouthern).mapToObj(i -> GeoData.generateSouthern());

		Stream<GeoData> geoDataStream = Stream.concat(northernStream, southernStream);

		log.info("Producing geo data to the input topic...");

		geoDataStream.map(converter::toJson).forEach(inputTopic::pipeInput);

		log.info("Consuming geo data statistics by hemisphere from output topic...");

		Map<String, Long> map = outputTopic.readKeyValuesToMap();

		log.info("{}", map);

		Assertions.assertEquals(numNorthern, map.get(GeoDataConfig.Key.NORTHERN_HEMISPHERE.keyValue()));
		Assertions.assertEquals(numSouthern, map.get(GeoDataConfig.Key.SOUTHERN_HEMISPHERE.keyValue()));

		Assertions.assertTrue(outputTopic.isEmpty());

	}

}
