package com.github.smeucci.geo.data.kafka.dsl;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
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
import com.github.smeucci.geo.data.kafka.utils.UtilityForTest;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CountEveryQuarterHourTest {

	private static final Logger log = LoggerFactory.getLogger(CountEveryQuarterHourTest.class);

	private TopologyTestDriver testDriver;
	private TestInputTopic<Long, String> inputTopic;
	private TestOutputTopic<String, Long> outputTopic;

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
	@DisplayName("Test Count Every Quarter Hour Topic")
	public void testCountEveryQuarterHourTopic() {

		log.info("==> testCountEveryQuarterHourTopic...");

		Instant start = LocalDateTime.of(2020, 1, 1, 00, 00).toInstant(ZoneOffset.UTC);

		int num = 61;

		Stream<TestRecord<Long, String>> geoDataStream = UtilityForTest.generateGeoDataStream(num, start, 60);

		log.info("Producing geo data to the input topic...");

		geoDataStream.forEachOrdered(inputTopic::pipeInput);

		log.info("Consuming quarter hour geo data stats from output topic...");

		List<KeyValue<String, Long>> records = outputTopic.readKeyValuesToList();

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

		log.info("Querying the geo data count every quarter hour by id state store");

		KeyValueStore<String, Long> store = testDriver
				.getKeyValueStore(GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR.storeName());

		KeyValueIterator<String, Long> iterator = store.all();

		int count = 0;

		while (iterator.hasNext()) {
			log.info("{}", iterator.next());
			count++;
		}

		iterator.close();

		Assertions.assertEquals(5, count);

	}

}
