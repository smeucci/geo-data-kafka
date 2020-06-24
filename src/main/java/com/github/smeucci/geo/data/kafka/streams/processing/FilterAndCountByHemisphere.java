package com.github.smeucci.geo.data.kafka.streams.processing;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.utils.GeoDataUtils;

public class FilterAndCountByHemisphere {

	private static final Logger log = LoggerFactory.getLogger(FilterAndCountByHemisphere.class);

	public static void northern(KStream<String, String> geoDataStream) {

		geoDataStream
				// keep northern hemisphere geo data
				.filter(GeoDataUtils.isInNorthernHemisphere)
				// peek geo data
				.peek((k, v) -> log.info("Northern Hemisphere: {}", v))
				// send to northern hemisphere topic
				.through(GeoDataConfig.Topic.NORTHERN_HEMISPHERE_GEO_DATA.topicName())
				// change key, use northern hemisphere
				.selectKey((k, v) -> GeoDataConfig.Key.NORTHERN_HEMISPHERE.keyValue())
				// group by key
				.groupByKey()
				// count northern hemisphere occurrences
				.count(Named.as("CountNorthernHemisphere"))
				// convert to stream
				.toStream()
				// send to hemisphere statistics topic
				.to(GeoDataConfig.Topic.HEMISPHERE_GEO_DATA_STATISTICS.topicName());

	}

	/**
	 * Filter and count southern geo data
	 * 
	 * @param geoDataStream The source geo data stream
	 */
	public static void southern(KStream<String, String> geoDataStream) {

		geoDataStream
				// keep southern hemisphere geo data
				.filter(GeoDataUtils.isInSouthernHemisphere)
				// peek geo data
				.peek((k, v) -> log.info("Southern Hemisphere: {}", v))
				// send to southern hemisphere topic
				.through(GeoDataConfig.Topic.SOUTHERN_HEMISPHERE_GEO_DATA.topicName())
				// change key, use southern hemisphere
				.selectKey((k, v) -> GeoDataConfig.Key.SOUTHERN_HEMISPHERE.keyValue())
				// group by key
				.groupByKey()
				// count southern hemisphere occurrences
				.count(Named.as("CountSouthernHemisphere"))
				// convert to stream
				.toStream()
				// send to hemisphere statistics topic
				.to(GeoDataConfig.Topic.HEMISPHERE_GEO_DATA_STATISTICS.topicName());

	}

}
