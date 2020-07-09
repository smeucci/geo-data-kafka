package com.github.smeucci.geo.data.kafka.dsl;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
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
import com.github.smeucci.geo.data.kafka.streams.dsl.CountEveryQuarterHour;
import com.github.smeucci.geo.data.kafka.topology.GeoDataTopology;
import com.github.smeucci.geo.data.kafka.utils.GeoDataUtils;
import com.github.smeucci.geo.data.kafka.utils.UtilityForTest;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CountEveryQuarterHourTest {

	private static final Logger log = LoggerFactory.getLogger(CountEveryQuarterHourTest.class);

	private TopologyTestDriver testDriver;
	private TestInputTopic<Long, String> inputTopic;
	private TestOutputTopic<Long, Long> outputTopic;

	@BeforeEach
	private void beforEach() {

		UtilityForTest.logStart();

		Properties properties = GeoDataConfig.testStreamsProperties();

		// build the topology
		Topology topology = new GeoDataTopology(Topic.SOURCE_GEO_DATA) //
				.addOperator(CountEveryQuarterHour::count) //
				.build();

		log.info("{}", topology.describe());

		// setup test driver
		testDriver = new TopologyTestDriver(topology, properties);

		// create test input topic
		inputTopic = testDriver.createInputTopic(GeoDataConfig.Topic.SOURCE_GEO_DATA.topicName(), new LongSerializer(),
				new StringSerializer());

		// create test output topic
		outputTopic = testDriver.createOutputTopic(GeoDataConfig.Topic.COUNT_EVERY_QUARTER_HOUR_GEO_DATA.topicName(),
				new LongDeserializer(), new LongDeserializer());

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
	@DisplayName("Test Count Every Quarter Hour Topic")
	public void testCountEveryQuarterHourTopic() {

		log.info("==> testCountEveryQuarterHourTopic...");

		Instant start = LocalDateTime.of(2020, 1, 1, 00, 00).toInstant(ZoneOffset.UTC);

		int num = 61;

		Stream<TestRecord<Long, String>> geoDataStream = UtilityForTest.generateGeoDataStream(num, start, 60);

		log.info("Producing geo data to the input topic...");

		geoDataStream.forEachOrdered(inputTopic::pipeInput);

		log.info("Consuming quarter hour geo data stats from output topic...");

		List<KeyValue<Long, Long>> records = outputTopic.readKeyValuesToList();

		Assertions.assertEquals(4, records.size());

		records.forEach(r -> {

			log.info("{}", r);

			Assertions.assertEquals(15, r.value);

		});

		Assertions.assertTrue(outputTopic.isEmpty());

	}

	@Test
	@Order(2)
	@DisplayName("Test Count Every Quarter Hour Store")
	public void testCountEveryQuarterHourStore() {

		log.info("==> testCountEveryQuarterHourTopic...");

		Instant start = LocalDateTime.of(2020, 1, 1, 00, 00).toInstant(ZoneOffset.UTC);

		int num = 61;

		Stream<TestRecord<Long, String>> geoDataStream = UtilityForTest.generateGeoDataStream(num, start, 60);

		log.info("Producing geo data to the input topic...");

		geoDataStream.forEach(inputTopic::pipeInput);

		log.info("Querying the geo data count every quarter hour state store");

		WindowStore<Long, ValueAndTimestamp<Long>> store = testDriver
				.getTimestampedWindowStore(GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR.storeName());

		// show all results

		log.info("-- Show all results by window:");

		KeyValueIterator<Windowed<Long>, ValueAndTimestamp<Long>> iterator = store.all();

		while (iterator.hasNext()) {

			KeyValue<Windowed<Long>, ValueAndTimestamp<Long>> keyVal = iterator.next();

			String aggKey = "[" + keyVal.key.window().startTime().toString() + ", "
					+ keyVal.key.window().endTime().toString() + "]";

			log.info("{}: {}", aggKey, keyVal.value.value());

		}

		iterator.close();

		// check window already closed

		ZonedDateTime queryTime = ZonedDateTime.of(2020, 1, 1, 00, 43, 23, 12565650, ZoneOffset.UTC);

		log.info("-- Query Time: {}", queryTime);

		Instant startWindow = GeoDataUtils.inferCurrentQuarterHourStartTime(queryTime.toInstant().toEpochMilli());

		log.info("Search Window: [{}, {}]", startWindow, startWindow.plus(15, ChronoUnit.MINUTES));

		ValueAndTimestamp<Long> firstWindowCount = store.fetch(startWindow.toEpochMilli(), startWindow.toEpochMilli());

		Assertions.assertNotNull(firstWindowCount);

		Assertions.assertNotNull(firstWindowCount.value());

		log.info("First Window Count: {}", firstWindowCount.value());

		Assertions.assertEquals(15, firstWindowCount.value());

		// check window still open

		queryTime = ZonedDateTime.of(2020, 1, 1, 01, 04, 43, 53265650, ZoneOffset.UTC);

		log.info("-- Query Time: {}", queryTime);

		startWindow = GeoDataUtils.inferCurrentQuarterHourStartTime(queryTime.toInstant().toEpochMilli());

		log.info("Search Window: [{}, {}]", startWindow, startWindow.plus(15, ChronoUnit.MINUTES));

		ValueAndTimestamp<Long> secondWindowCount = store.fetch(startWindow.toEpochMilli(), startWindow.toEpochMilli());

		Assertions.assertNotNull(secondWindowCount);

		Assertions.assertNotNull(secondWindowCount.value());

		log.info("Second Window Count: {}", secondWindowCount.value());

		Assertions.assertEquals(1, secondWindowCount.value());

	}

}
