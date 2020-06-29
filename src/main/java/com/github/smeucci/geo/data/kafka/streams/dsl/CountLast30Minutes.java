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

public class CountLast30Minutes {

	public static void count(final KStream<Long, String> geoDataStream) {

		WindowBytesStoreSupplier countLast30MinutesStoreSupplier = Stores.persistentWindowStore(
				GeoDataConfig.Store.COUNT_LAST_30_MINUTES.storeName(), Duration.ofDays(7), Duration.ofMinutes(30),
				false);

		geoDataStream
				// group by key, i.e. the id of the geo data record
				.groupByKey(Grouped.as(GeoDataConfig.Operator.GROUP_BY_GEO_DATA_ID.operatorName()))
				// set hopping window of size 30 min with hop size of 1 min
				.windowedBy(TimeWindows.of(Duration.ofMinutes(30)).advanceBy(Duration.ofMinutes(1)))
				// count occurrences in windows by group
				.count(Named.as(GeoDataConfig.Operator.COUNT_LAST_30_MINUTES.operatorName()),
						Materialized.as(countLast30MinutesStoreSupplier));

	}

}
