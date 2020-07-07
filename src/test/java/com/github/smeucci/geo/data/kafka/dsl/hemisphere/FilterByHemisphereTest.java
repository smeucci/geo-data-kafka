package com.github.smeucci.geo.data.kafka.dsl.hemisphere;

import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
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
import com.github.smeucci.geo.data.kafka.streams.dsl.hemisphere.FilterByHemisphere;
import com.github.smeucci.geo.data.kafka.topology.GeoDataTopology;
import com.github.smeucci.geo.data.kafka.utils.UtilityForTest;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FilterByHemisphereTest {

	private static final Logger log = LoggerFactory.getLogger(FilterByHemisphereTest.class);

	private TopologyTestDriver testDriver;
	private TestInputTopic<Long, String> inputTopic;
	private TestOutputTopic<Long, String> northernOutputTopic;
	private TestOutputTopic<Long, String> southernOutputTopic;

	@BeforeEach
	private void beforEach() {

		UtilityForTest.logStart();

		Properties properties = GeoDataConfig.testStreamsProperties();

		// build the topology
		Topology topology = new GeoDataTopology(Topic.SOURCE_GEO_DATA) //
				.addOperator(FilterByHemisphere::northern) //
				.addOperator(FilterByHemisphere::southern) //
				.build();

		log.info("{}", topology.describe());

		// setup test driver
		testDriver = new TopologyTestDriver(topology, properties);

		// create test input topic
		inputTopic = testDriver.createInputTopic(GeoDataConfig.Topic.SOURCE_GEO_DATA.topicName(), new LongSerializer(),
				new StringSerializer());

		// create test output
		northernOutputTopic = testDriver.createOutputTopic(GeoDataConfig.Topic.NORTHERN_HEMISPHERE_GEO_DATA.topicName(),
				new LongDeserializer(), new StringDeserializer());

		southernOutputTopic = testDriver.createOutputTopic(GeoDataConfig.Topic.SOUTHERN_HEMISPHERE_GEO_DATA.topicName(),
				new LongDeserializer(), new StringDeserializer());

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

		Stream<TestRecord<Long, String>> geoDataStream = UtilityForTest.generateGeoDataStream(numNorthern, numSouthern);

		log.info("Producing geo data to the input topic...");

		geoDataStream.forEach(inputTopic::pipeInput);

		log.info("Consuming geo data statistics by hemisphere from output topic...");

		List<String> northernList = northernOutputTopic.readValuesToList();
		List<String> southernList = southernOutputTopic.readValuesToList();

		log.info("Retrieved geo data for northern hemisphere: {}", northernList.size());
		log.info("Retrieved geo data for southern hemisphere: {}", southernList.size());

		Assertions.assertEquals(numNorthern, northernList.size());
		Assertions.assertEquals(numSouthern, southernList.size());

		Assertions.assertTrue(northernOutputTopic.isEmpty());
		Assertions.assertTrue(southernOutputTopic.isEmpty());

	}

}
