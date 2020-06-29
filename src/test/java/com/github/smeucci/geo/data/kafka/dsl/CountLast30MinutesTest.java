package com.github.smeucci.geo.data.kafka.dsl;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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

	private static final Logger log = LoggerFactory.getLogger(CountLast30MinutesTest.class);

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

		Instant start = LocalDateTime.of(2020, 1, 1, 00, 00).toInstant(ZoneOffset.UTC);

		int num = 60;

		Stream<TestRecord<Long, String>> geoDataStream = UtilityForTest.generateGeoDataStream(num, start, 60);

		log.info("Producing geo data to the input topic...");

		geoDataStream.forEach(inputTopic::pipeInput);

		log.info("Querying the geo data count last 30 minutes state store");

		WindowStore<Long, Long> store = testDriver
				.getWindowStore(GeoDataConfig.Store.COUNT_LAST_30_MINUTES.storeName());

		Assertions.assertTrue(store.persistent());

		// check window already closed

		ZonedDateTime queryTime = ZonedDateTime.of(2020, 1, 1, 00, 59, 23, 12565650, ZoneOffset.UTC);

		log.info("Query Time: {}", queryTime);

		ZonedDateTime to = queryTime.minusMinutes(30);
		ZonedDateTime from = to.withSecond(0).withNano(0);

		log.info("Search Window: [{}, {}]", from, to);

		KeyValueIterator<Windowed<Long>, Long> firstWindowTterator = store.fetchAll(from.toInstant(), to.toInstant());

		log.info("Iterating over select window entries...");

		long firstWindowCount = 0;

		while (firstWindowTterator.hasNext()) {
			KeyValue<Windowed<Long>, Long> wKeyValue = firstWindowTterator.next();
			firstWindowCount += wKeyValue.value;
		}

		firstWindowTterator.close();

		log.info("First Window Count: {}", firstWindowCount);

		Assertions.assertEquals(30, firstWindowCount);

		// check window still open

		queryTime = ZonedDateTime.of(2020, 1, 1, 01, 18, 43, 53265650, ZoneOffset.UTC);

		log.info("Query Time: {}", queryTime);

		to = queryTime.minusMinutes(30);
		from = to.withSecond(0).withNano(0);

		log.info("Search Window: [{}, {}]", from, to);

		KeyValueIterator<Windowed<Long>, Long> secondWindowTterator = store.fetchAll(from.toInstant(), to.toInstant());

		log.info("Iterating over selected window entries...");

		long secondWindowCount = 0;

		while (secondWindowTterator.hasNext()) {
			KeyValue<Windowed<Long>, Long> wKeyValue = secondWindowTterator.next();
			secondWindowCount += wKeyValue.value;
		}

		secondWindowTterator.close();

		log.info("First Window Count: {}", secondWindowCount);

		Assertions.assertEquals(12, secondWindowCount);

	}

}
