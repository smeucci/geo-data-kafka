package com.github.smeucci.geo.data.kafka.streams.dsl;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.utils.GeoDataUtils;

public class FilterByHemisphere {

	private static final Logger log = LoggerFactory.getLogger(FilterByHemisphere.class);

	public static void northern(final KStream<String, String> geoDataStream) {

		// filter for northern hemisphere geo data
		geoDataStream
				// keep northern hemisphere geo data
				.filter(GeoDataUtils.isInNorthernHemisphere,
						Named.as(GeoDataConfig.Processor.FILTER_NORTHERN.processorName()))
				// peek geo data
				.peek((k, v) -> log.info("Northern Hemisphere: {}", v))
				// set output topic
				.to(GeoDataConfig.Topic.NORTHERN_HEMISPHERE_GEO_DATA.topicName());

	}

	public static void southern(final KStream<String, String> geoDataStream) {

		// filter for northern hemisphere geo data
		geoDataStream
				// keep northern hemisphere geo data
				.filter(GeoDataUtils.isInSouthernHemisphere,
						Named.as(GeoDataConfig.Processor.FILTER_SOUTHERN.processorName()))
				// peek geo data
				.peek((k, v) -> log.info("Southern Hemisphere: {}", v))
				// set output topic
				.to(GeoDataConfig.Topic.SOUTHERN_HEMISPHERE_GEO_DATA.topicName());

	}

}
