package com.github.smeucci.geo.data.kafka.dsl;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
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
import com.github.smeucci.geo.data.kafka.streams.dsl.CountByHemisphere;
import com.github.smeucci.geo.data.kafka.topology.GeoDataTopology;
import com.github.smeucci.geo.data.kafka.utils.UtilityForTest;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CountByHemisphereTest {

	private static final Logger log = LoggerFactory.getLogger(CountByHemisphereTest.class);

	private static final GeoDataConverter converter = new GeoDataConverter();

	private TopologyTestDriver testDriver;
	private TestInputTopic<String, String> inputTopic;
	private TestOutputTopic<String, Long> outputTopic;

	@BeforeEach
	private void beforEach() {

		UtilityForTest.logStart();

		Properties properties = GeoDataConfig.testStreamsProperties();

		// build the topology
		Topology topology = new GeoDataTopology(Topic.SOURCE_GEO_DATA) //
				.addOperator(CountByHemisphere::count) //
				.build();

		log.info("{}", topology.describe());

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

		UtilityForTest.logEnd();

	}

	@Test
	@Order(1)
	@DisplayName("Test Count By Hemisphere Topic")
	public void testCountByHemisphereTopic() {

		log.info("==> testCountByHemisphere...");

		int numNorthern = 15;
		int numSouthern = 10;

		Stream<GeoData> geoDataStream = UtilityForTest.generateGeoDataStream(numNorthern, numSouthern);

		log.info("Producing geo data to the input topic...");

		geoDataStream.map(converter::toJson).forEach(inputTopic::pipeInput);

		log.info("Consuming geo data statistics by hemisphere from output topic...");

		Map<String, Long> map = outputTopic.readKeyValuesToMap();

		log.info("{}", map);

		Assertions.assertEquals(numNorthern, map.get(GeoDataConfig.Key.NORTHERN_HEMISPHERE.keyValue()));
		Assertions.assertEquals(numSouthern, map.get(GeoDataConfig.Key.SOUTHERN_HEMISPHERE.keyValue()));

		Assertions.assertTrue(outputTopic.isEmpty());

	}

	@Test
	@Order(2)
	@DisplayName("Test Count By Hemisphere State Store")
	public void testCountByHemisphereStateStore() {

		log.info("==> testCountByHemisphereStateStore");

		int numNorthern = 15;
		int numSouthern = 10;

		Stream<GeoData> geoDataStream = UtilityForTest.generateGeoDataStream(numNorthern, numSouthern);

		log.info("Producing geo data to the input topic...");

		geoDataStream.map(converter::toJson).forEach(inputTopic::pipeInput);

		log.info("Querying the geo data statistics state store");

		KeyValueStore<String, Long> store = testDriver
				.getKeyValueStore(GeoDataConfig.Store.COUNT_BY_HEMISPHERE.storeName());

		Assertions.assertTrue(store.persistent());

		log.info("Number of entries in store: {}", store.approximateNumEntries());

		KeyValueIterator<String, Long> iterator = store.all();

		log.info("Iterating over store entries...");

		while (iterator.hasNext()) {
			log.info("{}", iterator.next());
		}

		iterator.close();

		Assertions.assertEquals(numNorthern, store.get(GeoDataConfig.Key.NORTHERN_HEMISPHERE.keyValue()));
		Assertions.assertEquals(numSouthern, store.get(GeoDataConfig.Key.SOUTHERN_HEMISPHERE.keyValue()));

		Assertions.assertFalse(outputTopic.isEmpty());

	}

}
