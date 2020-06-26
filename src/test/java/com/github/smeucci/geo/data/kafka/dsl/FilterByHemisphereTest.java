package com.github.smeucci.geo.data.kafka.dsl;

import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
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
import com.github.smeucci.geo.data.kafka.config.GeoDataConfig.Topic;
import com.github.smeucci.geo.data.kafka.converter.GeoDataConverter;
import com.github.smeucci.geo.data.kafka.record.GeoData;
import com.github.smeucci.geo.data.kafka.streams.dsl.FilterByHemisphere;
import com.github.smeucci.geo.data.kafka.topology.GeoDataTopology;
import com.github.smeucci.geo.data.kafka.utils.UtilityForTest;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FilterByHemisphereTest {

	private static final Logger log = LoggerFactory.getLogger(FilterByHemisphereTest.class);

	private static final GeoDataConverter converter = new GeoDataConverter();

	private TopologyTestDriver testDriver;
	private TestInputTopic<String, String> inputTopic;
	private TestOutputTopic<String, String> northernOutputTopic;
	private TestOutputTopic<String, String> southernOutputTopic;

	@BeforeEach
	private void beforEach() {

		UtilityForTest.logStart();

		Properties properties = GeoDataConfig.testStreamsProperties();

		// build the topology
		Topology topology = new GeoDataTopology(Topic.SOURCE_GEO_DATA) //
				.addProcessor(FilterByHemisphere::northern) //
				.addProcessor(FilterByHemisphere::southern) //
				.build();

		log.info("{}", topology.describe());

		// setup test driver
		testDriver = new TopologyTestDriver(topology, properties);

		// create test input topic
		inputTopic = testDriver.createInputTopic(GeoDataConfig.Topic.SOURCE_GEO_DATA.topicName(),
				new StringSerializer(), new StringSerializer());

		// create test output
		northernOutputTopic = testDriver.createOutputTopic(GeoDataConfig.Topic.NORTHERN_HEMISPHERE_GEO_DATA.topicName(),
				new StringDeserializer(), new StringDeserializer());

		southernOutputTopic = testDriver.createOutputTopic(GeoDataConfig.Topic.SOUTHERN_HEMISPHERE_GEO_DATA.topicName(),
				new StringDeserializer(), new StringDeserializer());

	}

	@AfterEach
	public void afterEach() {

		if (this.testDriver != null) {
			this.testDriver.close();
		}

		UtilityForTest.logEnd();

	}

	@Test
	@Order(1)
	@DisplayName("Test Filter By Hemisphere")
	public void testFilterByHemisphereTopic() {

		log.info("==> testFilterByHemisphereTopic");

		int numNorthern = 15;
		int numSouthern = 10;

		Stream<GeoData> geoDataStream = UtilityForTest.generateGeoDataStream(numNorthern, numSouthern);

		log.info("Producing geo data to the input topic...");

		geoDataStream.map(converter::toJson).forEach(inputTopic::pipeInput);

		log.info("Consuming geo data statistics by hemisphere from output topic...");

		List<String> northernList = northernOutputTopic.readValuesToList();
		List<String> southernList = southernOutputTopic.readValuesToList();

		log.info("Retrieve geo data for northern hemisphere: {}", northernList.size());
		log.info("Retrieve geo data for southern hemisphere: {}", southernList.size());

		Assertions.assertEquals(numNorthern, northernList.size());
		Assertions.assertEquals(numSouthern, southernList.size());

		Assertions.assertTrue(northernOutputTopic.isEmpty());
		Assertions.assertTrue(southernOutputTopic.isEmpty());

	}

}
