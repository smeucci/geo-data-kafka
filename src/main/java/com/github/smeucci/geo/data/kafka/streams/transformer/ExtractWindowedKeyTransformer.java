package com.github.smeucci.geo.data.kafka.streams.transformer;

import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.utils.GeoDataUtils;

public class ExtractWindowedKeyTransformer implements Transformer<Windowed<Long>, Long, KeyValue<Long, Long>> {

	private static final Logger log = LoggerFactory.getLogger(ExtractWindowedKeyTransformer.class);

	private KeyValueStore<String, Long> bookmarkStore;

	private TimestampedWindowStore<Long, Long> store;

	private Long bookmark;

	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {

		// get bookmark store
		this.bookmarkStore = (KeyValueStore<String, Long>) context
				.getStateStore(GeoDataConfig.Store.TASK_BOOKMARK.storeName());

		// get the window state store
		this.store = (TimestampedWindowStore<Long, Long>) context
				.getStateStore(GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR.storeName());

		bookmark = this.bookmarkStore.get(context.taskId().toString());

		if (bookmark == null) {
			bookmark = Instant.EPOCH.toEpochMilli();
		}

		// punctuate scheduled every window size + grace size minutes
		context.schedule(Duration.ofMinutes(15), PunctuationType.STREAM_TIME, timestamp -> {

			log.info("-- Punctuate Time: {}, {}, task {}", Instant.ofEpochMilli(timestamp), timestamp,
					context.taskId());

			Instant currentQuarter = GeoDataUtils.inferCurrentQuarterHourStartTime(timestamp);

			log.info("Current bookmark: {} - {}", Instant.ofEpochMilli(bookmark), bookmark);

			this.store.fetchAll(bookmark, currentQuarter.toEpochMilli());

			KeyValueIterator<Windowed<Long>, ValueAndTimestamp<Long>> iterator = this.store.fetchAll(bookmark,
					currentQuarter.toEpochMilli());

			while (iterator.hasNext()) {

				KeyValue<Windowed<Long>, ValueAndTimestamp<Long>> kv = iterator.next();

				if (kv.key.key() >= bookmark && kv.key.key() < currentQuarter.toEpochMilli()) {

					log.info("{}: {}", kv.key.key(), kv.value.value());

					context.forward(kv.key.key(), kv.value.value());

					context.commit();

				}

			}

			iterator.close();

			// update bookmark
			bookmark = currentQuarter.toEpochMilli();
			this.bookmarkStore.put(context.taskId().toString(), bookmark);

		});

	}

	@Override
	public KeyValue<Long, Long> transform(Windowed<Long> key, Long value) {
		return null;
	}

	@Override
	public void close() {
		// Not needed
	}

}
