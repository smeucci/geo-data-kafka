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
import com.github.smeucci.geo.data.kafka.record.CountWrapper;
import com.github.smeucci.geo.data.kafka.serde.CountWrapperSerde;
import com.github.smeucci.geo.data.kafka.streams.transformer.AggregateQuarterHourByWindowTransformer;
import com.github.smeucci.geo.data.kafka.streams.transformer.ExtractWindowedKeyTransformer;
import com.github.smeucci.geo.data.kafka.streams.transformer.ExtractWindowedKeyTransformer2;
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
				.count(Named.as(GeoDataConfig.Operator.COUNT_EVERY_QUARTER_HOUR_BY_ID.operatorName()),
						Materialized.as(countEveryQuarterHourByIdStoreSupplier));

	}

	/**
	 * <p>
	 * Questo operatore è migliore di countOld, poichè wrappa i vari gruppi (creati sulla base del quarto d'ora
	 * corrispondente) in una window in modo da poter effettuare l'aggregazione sfruttando come uno window state store.
	 * </p>
	 * <p>
	 * In questo modo è possibile settare la retention per lo store e i record più vecchi vengono automaticamente
	 * scartati. A regime, implica che lo store avrà una dimensione stabile.
	 * </p>
	 * <p>
	 * Inoltre, in questo caso, l'uso del transformer per fare la punctuate ed emettere i risultati finali per una
	 * finestra al topic downstream non crea problemi essendoci stato un ripartizionamento per una chiave che
	 * corrisponde alla window su cui viene fatta la count. Quindi tutti i record con diversi id, collassano su un'unica
	 * chiave che corrisponderà all'inizio della window che verrà applicata successivamente. In questo modo i dati sono
	 * partizionati per finestre temporali cosicché ciascuna istanza ha l'esclusività su una determinata finestra.
	 * </p>
	 * 
	 * @param geoDataStream
	 */
	public static void count(final KStream<Long, String> geoDataStream) {

		WindowBytesStoreSupplier countEveryQuarterHourStoreSupplier = Stores.inMemoryWindowStore(
				GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR.storeName(), Duration.ofDays(1), Duration.ofMinutes(15),
				false);

		geoDataStream
				// change key -> time interval it belongs to
				.selectKey((k, v) -> GeoDataUtils.getQuarterHourStartWindowAsLong(v),
						Named.as(GeoDataConfig.Operator.SELECT_KEY_QUARTER_HOUR.operatorName()))
				// group by key
				.groupByKey(Grouped.with(GeoDataConfig.Operator.GROUP_BY_QUARTER_HOUR.operatorName(), Serdes.Long(),
						Serdes.String()))
				// set hopping window of size 15 min with implicit hop size of 15 min, no overlap
				.windowedBy(TimeWindows.of(Duration.ofMinutes(15)).grace(Duration.ZERO))
				// count occurrences in windows
				.count(Named.as(GeoDataConfig.Operator.COUNT_EVERY_QUARTER_HOUR.operatorName()),
						Materialized.as(countEveryQuarterHourStoreSupplier)) // TODO reduce flag committato oggetto
																				// composito + wall clock punctuate
				// to stream
				.toStream(Named.as(GeoDataConfig.Operator.TO_COUNT_EVERY_QUARTER_HOUR_STREAM.operatorName()))
				// to topic
				.transform(() -> new ExtractWindowedKeyTransformer(),
						Named.as(GeoDataConfig.Operator.EXTRACT_WINDOWED_KEY_TRANSFORMER.operatorName()),
						GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR.storeName(),
						GeoDataConfig.Store.TASK_BOOKMARK.storeName())
				// to topic
				.to(GeoDataConfig.Topic.COUNT_EVERY_QUARTER_HOUR_GEO_DATA.topicName(),
						Produced.with(Serdes.Long(), Serdes.Long())
								.withName(GeoDataConfig.Operator.TO_COUNT_EVERY_QUARTER_HOUR_TOPIC.operatorName()));

	}

	/**
	 * Non viene usato il bookmark store
	 * 
	 * @param geoDataStream
	 */
	public static void count2(final KStream<Long, String> geoDataStream) {

		WindowBytesStoreSupplier countEveryQuarterHourStoreSupplier = Stores.inMemoryWindowStore(
				GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR.storeName(), Duration.ofDays(1), Duration.ofMinutes(15),
				false);

		geoDataStream
				// change key -> time interval it belongs to
				.selectKey((k, v) -> GeoDataUtils.getQuarterHourStartWindowAsLong(v),
						Named.as(GeoDataConfig.Operator.SELECT_KEY_QUARTER_HOUR.operatorName()))
				// map values
				.mapValues((v) -> new CountWrapper(),
						Named.as(GeoDataConfig.Operator.MAP_VALUES_COUNT_WRAPPER.operatorName()))
				// group by key
				.groupByKey(Grouped.with(GeoDataConfig.Operator.GROUP_BY_QUARTER_HOUR.operatorName(), Serdes.Long(),
						Serdes.serdeFrom(new CountWrapperSerde(), new CountWrapperSerde())))
				// set hopping window of size 15 min with implicit hop size of 15 min, no overlap
				.windowedBy(TimeWindows.of(Duration.ofMinutes(15)).grace(Duration.ZERO))
				// aggregate occurrences in windows
				.reduce((aggValue, newValue) -> aggValue.plusOne(),
						Named.as(GeoDataConfig.Operator.REDUCE_EVERY_QUARTER_HOUR.operatorName()),
						Materialized.as(countEveryQuarterHourStoreSupplier))
				// to stream
				.toStream(Named.as(GeoDataConfig.Operator.TO_COUNT_EVERY_QUARTER_HOUR_STREAM.operatorName()))
				// to topic
				.transform(() -> new ExtractWindowedKeyTransformer2(),
						Named.as(GeoDataConfig.Operator.EXTRACT_WINDOWED_KEY_TRANSFORMER.operatorName()),
						GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR.storeName())
				// to topic
				.to(GeoDataConfig.Topic.COUNT_EVERY_QUARTER_HOUR_GEO_DATA.topicName(),
						Produced.with(Serdes.Long(), Serdes.Long())
								.withName(GeoDataConfig.Operator.TO_COUNT_EVERY_QUARTER_HOUR_TOPIC.operatorName()));

	}

	/**
	 * <p>
	 * Questo operatore richiede l'utilizzo di un key value store, che per definizione ha retention infinita.
	 * </p>
	 * <p>
	 * Significa che lo store cresce in dimensioni indefinitivamente se non c'è qualcuno che lo svuota. Richiederebbe
	 * l'utilizzo di process o tranform per effettuare una punctuate che pulisce i dati che hanno superato il periodo di
	 * retention desiderato.
	 * </p>
	 * 
	 * @param geoDataStream
	 * @deprecated
	 */
	@Deprecated
	public static void countOld(final KStream<Long, String> geoDataStream) {

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
				.count(Named.as(GeoDataConfig.Operator.COUNT_EVERY_QUARTER_HOUR.operatorName()),
						Materialized.as(countEveryQuarterHourStoreSupplier))
				// suppress results for window size, starting when first key for a window is processed
				.suppress(Suppressed.untilTimeLimit(Duration.ofMinutes(15), Suppressed.BufferConfig.unbounded()))
				// to stream
				.toStream(Named.as(GeoDataConfig.Operator.TO_COUNT_EVERY_QUARTER_HOUR_STREAM.operatorName()))
				// to topic
				.to(GeoDataConfig.Topic.COUNT_EVERY_QUARTER_HOUR_GEO_DATA.topicName(),
						Produced.as(GeoDataConfig.Operator.TO_COUNT_EVERY_QUARTER_HOUR_TOPIC.operatorName()));

	}

	/**
	 * <p>
	 * In questo caso, l'utilizzo del transformere sulle window dei vari gruppi (raggruppati per id) comporta che, data
	 * una finestra di cui vogliamo i risultati finali, i risultati siano sparpagliati sulle eventuali istanze
	 * dell'applicazione.
	 * </p>
	 * <p>
	 * Questo poichè il partizionamente è su base id del record, quindi ciascun task e di conseguenza ciascuna possibile
	 * instanza remota contengono un sottoinsieme del conteggio per id data una finestra. Anche implementando un
	 * servizio rest che raccoglie i dati dagli store remoti, l'operazione sarebbe incorretta perchè ripetuta da
	 * ciascuna instanza, di fatto emittendo più volte lo stesso risultato sul topic downstream.
	 * </p>
	 * 
	 * @param geoDataStream
	 * @deprecated
	 */
	@Deprecated
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
				.count(Named.as(GeoDataConfig.Operator.COUNT_EVERY_QUARTER_HOUR_BY_ID.operatorName()),
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
