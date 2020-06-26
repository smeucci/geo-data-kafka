package com.github.smeucci.geo.data.kafka.streams.dsl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.utils.GeoDataUtils;

public class CountByHemisphere {

	public static void count(KStream<String, String> geoDataStream) {

		KeyValueBytesStoreSupplier countByHemisphereStoreSupplier = Stores
				.persistentKeyValueStore(GeoDataConfig.Store.COUNT_BY_HEMISPHERE.storeName());

		// count geo data occurrences by hemisphere
		KStream<String, Long> hemisphereStatsStream = geoDataStream
				// geo data with latitude == 0 doesn't belong to either hemisphere
				.filterNot(GeoDataUtils.isInEquator, Named.as(GeoDataConfig.Processor.FILTER_EQUATOR.processorName()))
				// change key, use hemisphere
				.selectKey(GeoDataUtils.keyForHemisphere,
						Named.as(GeoDataConfig.Processor.SELECT_KEY_HEMISPHERE.processorName()))
				// group by key
				.groupByKey()
				// count occurrences for each hemisphere
				.count(Named.as(GeoDataConfig.Processor.COUNT_BY_HEMISPHRE.processorName()),
						Materialized.as(countByHemisphereStoreSupplier))
				// convert to stream
				.toStream();

		// set output topic
		hemisphereStatsStream.to(GeoDataConfig.Topic.HEMISPHERE_GEO_DATA_STATISTICS.topicName(),
				Produced.valueSerde(Serdes.Long()));

	}

}
