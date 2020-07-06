package com.github.smeucci.geo.data.kafka.streams.dsl;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.streams.transformer.AggregateQuarterHourByWindowTransformer;
import com.github.smeucci.geo.data.kafka.utils.GeoDataUtils;

public class CountEveryQuarterHour {

	public static void countById(final KStream<Long, String> geoDataStream) {

		WindowBytesStoreSupplier countEveryQuarterHourByIdStoreSupplier = Stores.inMemoryWindowStore(
				GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR_BY_ID.storeName(), Duration.ofDays(1),
				Duration.ofMinutes(15), false);

		geoDataStream
				// group by key, i.e. the id of the geo data record
				.groupByKey(Grouped.as(GeoDataConfig.Operator.GROUP_BY_GEO_DATA_ID.operatorName()))
				// set hopping window of size 15 min with implicit hop size of 15 min, no overlap
				.windowedBy(TimeWindows.of(Duration.ofMinutes(15)))
				// count occurrences in windows by group
				.count(Named.as(GeoDataConfig.Operator.COUNT_EVERY_QUARTES_HOUR_BY_ID.operatorName()),
						Materialized.as(countEveryQuarterHourByIdStoreSupplier));

	}

	public static void count(final KStream<Long, String> geoDataStream) {

		KeyValueBytesStoreSupplier countEveryQuarterHourStoreSupplier = Stores
				.inMemoryKeyValueStore(GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR.storeName());

		geoDataStream
				// change key -> time interval it belongs to
				.selectKey((k, v) -> GeoDataUtils.getQuarterHourWindowAsString(v),
						Named.as(GeoDataConfig.Operator.SELECT_KEY_QUARTER_HOUR.operatorName()))
				// group by key
				.groupByKey(Grouped.with(GeoDataConfig.Operator.GROUP_BY_QUARTER_HOUR.operatorName(), Serdes.String(),
						Serdes.String()))
				// count occurrences for each key, i.e. for each quarter hour
				.count(Named.as(GeoDataConfig.Operator.COUNT_EVERY_QUARTES_HOUR.operatorName()),
						Materialized.as(countEveryQuarterHourStoreSupplier))
				// suppress results for window size, starting when first key for a window is processed
				.suppress(Suppressed.untilTimeLimit(Duration.ofMinutes(15), Suppressed.BufferConfig.unbounded()))
				// to stream
				.toStream(Named.as(GeoDataConfig.Operator.TO_COUNT_EVERY_QUARTER_HOUR_STREAM.operatorName()))
				// to topic
				.to(GeoDataConfig.Topic.COUNT_EVERY_QUARTER_HOUR_GEO_DATA.topicName(),
						Produced.as(GeoDataConfig.Operator.TO_COUNT_EVERY_QUARTER_HOUR_TOPIC.operatorName()));

	}

	public static void countByIdAndAggregate(final KStream<Long, String> geoDataStream) {

		WindowBytesStoreSupplier countEveryQuarterHourByIdStoreSupplier = Stores.inMemoryWindowStore(
				GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR_BY_ID.storeName(), Duration.ofDays(1),
				Duration.ofMinutes(15), false);

		// count by id for each window
		KTable<Windowed<Long>, Long> countByIdTable = geoDataStream
				// group by key, i.e. the id of the geo data record
				.groupByKey(Grouped.with(GeoDataConfig.Operator.GROUP_BY_GEO_DATA_ID.operatorName(), Serdes.Long(),
						Serdes.String()))
				// set hopping window of size 15 min with implicit hop size of 15 min, no overlap
				.windowedBy(TimeWindows.of(Duration.ofMinutes(15)).grace(Duration.ZERO))
				// count occurrences in windows by group
				.count(Named.as(GeoDataConfig.Operator.COUNT_EVERY_QUARTES_HOUR_BY_ID.operatorName()),
						Materialized.as(countEveryQuarterHourByIdStoreSupplier));

		// aggregate (sum) results by window for each id
		countByIdTable
				// to stream
				.toStream(Named.as(GeoDataConfig.Operator.TO_COUNT_EVERY_QUARTER_HOUR_STREAM.operatorName()))
				// to topic
				.transform(() -> new AggregateQuarterHourByWindowTransformer(),
						Named.as(GeoDataConfig.Operator.AGGREGATE_BY_WINDOW_TRANSFORMER.operatorName()),
						GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR_BY_ID.storeName())
				// to topic
				.to(GeoDataConfig.Topic.COUNT_EVERY_QUARTER_HOUR_GEO_DATA.topicName(),
						Produced.with(Serdes.Long(), Serdes.Long())
								.withName(GeoDataConfig.Operator.TO_COUNT_EVERY_QUARTER_HOUR_TOPIC.operatorName()));

	}

}
