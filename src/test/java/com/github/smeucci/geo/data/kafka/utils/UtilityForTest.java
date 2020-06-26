package com.github.smeucci.geo.data.kafka.utils;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.smeucci.geo.data.kafka.record.GeoData;

public class UtilityForTest {

	private static final Logger log = LoggerFactory.getLogger(UtilityForTest.class);

	public static Stream<GeoData> generateGeoDataStream(int numNorthern, int numSouthern) {

		log.info("Generating {} northern hemisphere geo data...", numNorthern);

		Stream<GeoData> northernStream = IntStream.range(0, numNorthern).mapToObj(i -> GeoData.generateNorthen());

		log.info("Generating {} southern hemisphere geo data...", numSouthern);

		Stream<GeoData> southernStream = IntStream.range(0, numSouthern).mapToObj(i -> GeoData.generateSouthern());

		Stream<GeoData> geoDataStream = Stream.concat(northernStream, southernStream);

		return geoDataStream;

	}

	public static void logStart() {
		log.info("============================================================");
		log.info("==================== S T A R T  T E S T ====================");
		log.info("============================================================");
	}

	public static void logEnd() {
		log.info("============================================================");
		log.info("============================================================");
	}

}
