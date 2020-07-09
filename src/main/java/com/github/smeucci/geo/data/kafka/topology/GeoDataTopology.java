package com.github.smeucci.geo.data.kafka.topology;

import java.util.function.Consumer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;

public class GeoDataTopology {

	private final KStream<Long, String> sourceStream;

	private final StreamsBuilder streamsBuilder;

	public GeoDataTopology(final GeoDataConfig.Topic topic) {

		streamsBuilder = new StreamsBuilder();

		addGlobalStore(GeoDataConfig.Store.TASK_BOOKMARK.storeName());

		sourceStream = streamsBuilder.stream(topic.topicName(),
				Consumed.as(GeoDataConfig.Operator.SOURCE_GEO_DATA.operatorName()));

	}

	public GeoDataTopology addOperator(final Consumer<KStream<Long, String>> processor) {

		processor.accept(sourceStream);

		return this;

	}

	public GeoDataTopology addBookmarkStore(String bookmarkStoreName) {

		KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(bookmarkStoreName);

		StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier,
				Serdes.String(), Serdes.Long());

		streamsBuilder.addStateStore(storeBuilder);

		return this;

	}

	public Topology build() {

		return streamsBuilder.build();

	}

	private void addGlobalStore(String storeName) {

		KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(storeName);

		StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier,
				Serdes.String(), Serdes.Long());

		streamsBuilder.addStateStore(storeBuilder);

	}

}
