package com.github.smeucci.geo.data.kafka.dsl;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
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
import com.github.smeucci.geo.data.kafka.streams.dsl.CountLast30Minutes;
import com.github.smeucci.geo.data.kafka.topology.GeoDataTopology;
import com.github.smeucci.geo.data.kafka.utils.UtilityForTest;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CountLast30MinutesTest {

	private static final Logger log = LoggerFactory.getLogger(CountByHemisphereTest.class);

	private TopologyTestDriver testDriver;
	private TestInputTopic<Long, String> inputTopic;

	@BeforeEach
	private void beforEach() {

		UtilityForTest.logStart();

		Properties properties = GeoDataConfig.testStreamsProperties();

		// build the topology
		Topology topology = new GeoDataTopology(Topic.SOURCE_GEO_DATA) //
				.addOperator(CountLast30Minutes::count) //
				.build();

		log.info("{}", topology.describe());

		// setup test driver
		testDriver = new TopologyTestDriver(topology, properties);

		// create test input topic
		inputTopic = testDriver.createInputTopic(GeoDataConfig.Topic.SOURCE_GEO_DATA.topicName(), new LongSerializer(),
				new StringSerializer());

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
	@DisplayName("Test Count Last 30 Minutes Store")
	public void testCountLast30MinutesStore() {

		log.info("==> testCountLast30MinutesStore...");

		int numNorthern = 15;
		int numSouthern = 10;

		Stream<TestRecord<Long, String>> geoDataStream = UtilityForTest.generateGeoDataStream(numNorthern, numSouthern);

		log.info("Producing geo data to the input topic...");

		geoDataStream.forEach(inputTopic::pipeInput);

		log.info("Querying the geo data statistics state store");

		WindowStore<Long, Long> store = testDriver
				.getWindowStore(GeoDataConfig.Store.COUNT_LAST_30_MINUTES.storeName());

		Assertions.assertTrue(store.persistent());

		Instant now = Instant.now();

		KeyValueIterator<Windowed<Long>, Long> iterator = store.fetchAll(now.minus(30, ChronoUnit.MINUTES), now);

		log.info("Iterating over store entries...");

		long totalCount = 0;

		while (iterator.hasNext()) {
			KeyValue<Windowed<Long>, Long> wKeyValue = iterator.next();
			log.info("{}", wKeyValue);
			totalCount += wKeyValue.value;
		}

		iterator.close();

		Assertions.assertEquals(numNorthern + numSouthern, totalCount);

	}

}
