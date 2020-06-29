package com.github.smeucci.geo.data.kafka.streams.dsl;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.utils.GeoDataUtils;

public class FilterByHemisphere {

	private static final Logger log = LoggerFactory.getLogger(FilterByHemisphere.class);

	public static void northern(final KStream<Long, String> geoDataStream) {

		// filter for northern hemisphere geo data
		geoDataStream
				// keep northern hemisphere geo data
				.filter(GeoDataUtils.isInNorthernHemisphere,
						Named.as(GeoDataConfig.Operator.FILTER_NORTHERN.operatorName()))
				// peek geo data
				.peek((k, v) -> log.info("Northern Hemisphere: {}", v))
				// set output topic
				.to(GeoDataConfig.Topic.NORTHERN_HEMISPHERE_GEO_DATA.topicName());

	}

	public static void southern(final KStream<Long, String> geoDataStream) {

		// filter for northern hemisphere geo data
		geoDataStream
				// keep northern hemisphere geo data
				.filter(GeoDataUtils.isInSouthernHemisphere,
						Named.as(GeoDataConfig.Operator.FILTER_SOUTHERN.operatorName()))
				// peek geo data
				.peek((k, v) -> log.info("Southern Hemisphere: {}", v))
				// set output topic
				.to(GeoDataConfig.Topic.SOUTHERN_HEMISPHERE_GEO_DATA.topicName());

	}

	@SuppressWarnings("unchecked")
	public static void branch(final KStream<Long, String> geoDataStream) {

		// filter for northern hemisphere geo data
		KStream<Long, String>[] branches = geoDataStream
				// branch by hemisphere
				.branch(Named.as(GeoDataConfig.Operator.BRANCH_BY_HEMISPHERE.operatorName()),
						GeoDataUtils.isInNorthernHemisphere, GeoDataUtils.isInSouthernHemisphere);

		branches[0]
				// peek geo data
				.peek((k, v) -> log.info("Northern Hemisphere: {}", v))
				// set output topic
				.to(GeoDataConfig.Topic.NORTHERN_HEMISPHERE_GEO_DATA.topicName());

		branches[1]
				// peek geo data
				.peek((k, v) -> log.info("Southern Hemisphere: {}", v))
				// set output topic
				.to(GeoDataConfig.Topic.SOUTHERN_HEMISPHERE_GEO_DATA.topicName());

	}

}
