package com.github.smeucci.geo.data.kafka.utils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.converter.GeoDataConverter;
import com.github.smeucci.geo.data.kafka.record.GeoData;

public class GeoDataUtils {

	private static final GeoDataConverter converter = new GeoDataConverter();

	/**
	 * Predicate is in northern hemisphere
	 */
	public static Predicate<Long, String> isInNorthernHemisphere = (key,
			geoData) -> GeoDataUtils.extractLatitude(geoData) > 0;

	/**
	 * Predicate is in southern hemisphere
	 */
	public static Predicate<Long, String> isInSouthernHemisphere = (key,
			geoData) -> GeoDataUtils.extractLatitude(geoData) < 0;

	/**
	 * Predicate is in equator
	 */
	public static Predicate<Long, String> isInEquator = (key, geoData) -> GeoDataUtils.extractLatitude(geoData) == 0;

	/**
	 * KeyValueMapper to get the key name for hemisphere
	 */
	public static KeyValueMapper<Long, String, String> keyForHemisphere = (key,
			geoData) -> isInNorthernHemisphere.test(key, geoData) ? GeoDataConfig.Key.NORTHERN_HEMISPHERE.keyValue()
					: GeoDataConfig.Key.SOUTHERN_HEMISPHERE.keyValue();

	public static double extractLatitude(final String geoDataJson) {

		Optional<GeoData> optGeoData = converter.fromJson(geoDataJson);

		return optGeoData.isPresent() ? optGeoData.get().latitude() : 0;

	}

	public static String getQuarterHourWindowAsString(String record) {

		Optional<GeoData> optGeoData = converter.fromJson(record);

		if (optGeoData.isEmpty()) {
			return null;
		}

		ZonedDateTime geoDataTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(optGeoData.get().timestamp()),
				ZoneOffset.UTC);

		int quarter = geoDataTime.getMinute() - (geoDataTime.getMinute() % 15);

		Instant windowStart = geoDataTime.withMinute(quarter).withSecond(0).withNano(0).toInstant();
		Instant windowEnd = windowStart.plus(15, ChronoUnit.MINUTES);

		String windowAsString = windowStart.toEpochMilli() + "/" + windowEnd.toEpochMilli();

		return windowAsString;

	}

}
