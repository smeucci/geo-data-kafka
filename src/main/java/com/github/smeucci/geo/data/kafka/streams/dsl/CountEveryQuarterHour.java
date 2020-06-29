package com.github.smeucci.geo.data.kafka.streams.dsl;

import java.time.Duration;

import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;

public class CountEveryQuarterHour {

	public static void count(final KStream<Long, String> geoDataStream) {

		WindowBytesStoreSupplier countEveryQuarterHourStoreSupplier = Stores.persistentWindowStore(
				GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR.storeName(), Duration.ofDays(1), Duration.ofMinutes(15),
				false);

		geoDataStream
				// group by key, i.e. the id of the geo data record
				.groupByKey(Grouped.as(GeoDataConfig.Operator.GROUP_BY_GEO_DATA_ID.operatorName()))
				// set hopping window of size 15 min with implicit hop size of 15 min, no overlap
				.windowedBy(TimeWindows.of(Duration.ofMinutes(15)))
				// count occurrences in windows by group
				.count(Named.as(GeoDataConfig.Operator.COUNT_EVERY_QUARTES_HOUR.operatorName()),
						Materialized.as(countEveryQuarterHourStoreSupplier));

	}

}
