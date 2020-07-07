package com.github.smeucci.geo.data.kafka.dsl;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
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
import com.github.smeucci.geo.data.kafka.utils.GeoDataUtils;
import com.github.smeucci.geo.data.kafka.utils.UtilityForTest;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CountLast30MinutesByIdTest {

	private static final Logger log = LoggerFactory.getLogger(CountLast30MinutesByIdTest.class);

	private TopologyTestDriver testDriver;
	private TestInputTopic<Long, String> inputTopic;

	@BeforeEach
	private void beforEach() {

		UtilityForTest.logStart();

		Properties properties = GeoDataConfig.testStreamsProperties();

		// build the topology
		Topology topology = new GeoDataTopology(Topic.SOURCE_GEO_DATA) //
				.addOperator(CountLast30Minutes::countById) //
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
	@DisplayName("Test Count Last 30 Minutes By Id Store")
	public void testCountLast30MinutesByIdStore() {

		log.info("==> testCountLast30MinutesByIdStore...");

		Instant start = LocalDateTime.of(2020, 1, 1, 00, 00).toInstant(ZoneOffset.UTC);

		int num = 60;

		Stream<TestRecord<Long, String>> geoDataStream = UtilityForTest.generateGeoDataStream(num, start, 60);

		log.info("Producing geo data to the input topic...");

		geoDataStream.forEach(inputTopic::pipeInput);

		log.info("Querying the geo data count last 30 minutes by id state store");

		WindowStore<Long, ValueAndTimestamp<Long>> store = testDriver
				.getTimestampedWindowStore(GeoDataConfig.Store.COUNT_LAST_30_MINUTES_BY_ID.storeName());

		// print all computed windows
		KeyValueIterator<Windowed<Long>, ValueAndTimestamp<Long>> iterator = store.all();

		Map<String, Long> aggregateResultsByWindow = new TreeMap<>();

		while (iterator.hasNext()) {

			KeyValue<Windowed<Long>, ValueAndTimestamp<Long>> keyVal = iterator.next();

			String aggKey = "[" + keyVal.key.window().startTime().toString() + ", "
					+ keyVal.key.window().endTime().toString() + "]";

			Long sum = aggregateResultsByWindow.get(aggKey);

			sum = sum == null ? keyVal.value.value() : sum + keyVal.value.value();

			aggregateResultsByWindow.put(aggKey, sum);

		}

		iterator.close();

		log.info("-- Show aggregated results by window:");
		aggregateResultsByWindow.entrySet().forEach(e -> log.info("{}", e));

		// check window already closed

		ZonedDateTime queryTime = ZonedDateTime.of(2020, 1, 1, 00, 59, 23, 12565650, ZoneOffset.UTC);

		log.info("-- Query Time: {}", queryTime);

		Instant startWindow = GeoDataUtils.inferHalfHourStartTimeFromQuery(queryTime.toInstant().toEpochMilli());

		log.info("Search Window: [{}, {}]", startWindow, startWindow.plus(30, ChronoUnit.MINUTES));

		KeyValueIterator<Windowed<Long>, ValueAndTimestamp<Long>> firstWindowIterator = store.fetchAll(startWindow,
				startWindow);

		log.info("Iterating over select window entries...");

		long firstWindowCount = 0;

		while (firstWindowIterator.hasNext()) {
			KeyValue<Windowed<Long>, ValueAndTimestamp<Long>> wKeyValue = firstWindowIterator.next();
			firstWindowCount += wKeyValue.value.value();
		}

		firstWindowIterator.close();

		log.info("First Window Count: {}", firstWindowCount);

		Assertions.assertEquals(30, firstWindowCount);

		// check window still open

		queryTime = ZonedDateTime.of(2020, 1, 1, 01, 18, 43, 53265650, ZoneOffset.UTC);

		log.info("-- Query Time: {}", queryTime);

		startWindow = GeoDataUtils.inferHalfHourStartTimeFromQuery(queryTime.toInstant().toEpochMilli());

		log.info("Search Window: [{}, {}]", startWindow, startWindow.plus(30, ChronoUnit.MINUTES));

		KeyValueIterator<Windowed<Long>, ValueAndTimestamp<Long>> secondWindowIterator = store.fetchAll(startWindow,
				startWindow);

		log.info("Iterating over selected window entries...");

		long secondWindowCount = 0;

		while (secondWindowIterator.hasNext()) {
			KeyValue<Windowed<Long>, ValueAndTimestamp<Long>> wKeyValue = secondWindowIterator.next();
			secondWindowCount += wKeyValue.value.value();
		}

		secondWindowIterator.close();

		log.info("Second Window Count: {}", secondWindowCount);

		Assertions.assertEquals(12, secondWindowCount);

	}

}
