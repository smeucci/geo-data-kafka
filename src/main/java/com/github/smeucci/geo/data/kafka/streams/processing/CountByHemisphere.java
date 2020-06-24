package com.github.smeucci.geo.data.kafka.streams.processing;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.utils.GeoDataUtils;

public class CountByHemisphere {

	public static void count(KStream<String, String> geoDataStream) {

		// count geo data occurrences by hemisphere
		KStream<String, Long> hemisphereStatsStream = geoDataStream
				// geo data with latitude == 0 doesn't belong to either hemisphere
				.filterNot((k, v) -> GeoDataUtils.extractLatitude(v) == 0)
				// change key, use hemisphere
				.selectKey(GeoDataUtils.keyForHemisphere)
				// group by key
				.groupByKey()
				// count occurrences for each hemisphere
				.count(Named.as("CountByHemisphere"))
				// convert to stream
				.toStream();

		// set output topic
		hemisphereStatsStream.to(GeoDataConfig.Topic.HEMISPHERE_GEO_DATA_STATISTICS.topicName(),
				Produced.valueSerde(Serdes.Long()));

	}

}
