package com.github.smeucci.geo.data.kafka.streams.dsl.hemisphere;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.utils.GeoDataUtils;

public class CountByHemisphere {

	public static void count(final KStream<Long, String> geoDataStream) {

		KeyValueBytesStoreSupplier countByHemisphereStoreSupplier = Stores
				.persistentKeyValueStore(GeoDataConfig.Store.COUNT_BY_HEMISPHERE.storeName());

		// count geo data occurrences by hemisphere
		geoDataStream
				// geo data with latitude == 0 doesn't belong to either hemisphere
				.filterNot(GeoDataUtils.isInEquator, Named.as(GeoDataConfig.Operator.FILTER_EQUATOR.operatorName()))
				// change key, use hemisphere
				.selectKey(GeoDataUtils.keyForHemisphere,
						Named.as(GeoDataConfig.Operator.SELECT_KEY_HEMISPHERE.operatorName()))
				// group by key
				.groupByKey(Grouped.with(GeoDataConfig.Operator.GROUP_BY_HEMISPHERE.operatorName(), Serdes.String(),
						Serdes.String()))
				// count occurrences for each hemisphere
				.count(Named.as(GeoDataConfig.Operator.COUNT_BY_HEMISPHERE.operatorName()),
						Materialized.as(countByHemisphereStoreSupplier))
				// convert to stream
				.toStream()
				// set output topic
				.to(GeoDataConfig.Topic.HEMISPHERE_GEO_DATA_STATISTICS.topicName(),
						Produced.with(Serdes.String(), Serdes.Long()));

	}

}
