package com.github.smeucci.geo.data.kafka.utils;

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
	public static Predicate<String, String> isInNorthernHemisphere = (key,
			geoData) -> GeoDataUtils.extractLatitude(geoData) > 0;

	/**
	 * Predicate is in southern hemisphere
	 */
	public static Predicate<String, String> isInSouthernHemisphere = (key,
			geoData) -> GeoDataUtils.extractLatitude(geoData) < 0;

	/**
	 * KeyValueMapper to get the key name for hemisphere
	 */
	public static KeyValueMapper<String, String, String> keyForHemisphere = (key,
			geoData) -> isInNorthernHemisphere.test(key, geoData) ? GeoDataConfig.Key.NORTHERN_HEMISPHERE.keyValue()
					: GeoDataConfig.Key.SOUTHERN_HEMISPHERE.keyValue();

	public static double extractLatitude(String geoDataJson) {

		Optional<GeoData> optGeoData = converter.fromJson(geoDataJson);

		return optGeoData.isPresent() ? optGeoData.get().latitude() : 0;

	}

}