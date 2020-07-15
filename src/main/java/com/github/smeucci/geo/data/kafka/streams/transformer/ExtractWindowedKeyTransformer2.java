package com.github.smeucci.geo.data.kafka.streams.transformer;

import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.record.CountWrapper;
import com.github.smeucci.geo.data.kafka.utils.GeoDataUtils;

public class ExtractWindowedKeyTransformer2 implements Transformer<Windowed<Long>, CountWrapper, KeyValue<Long, Long>> {

	private static final Logger log = LoggerFactory.getLogger(ExtractWindowedKeyTransformer2.class);

	private TimestampedWindowStore<Long, CountWrapper> store;

	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {

		// get the window state store
		this.store = (TimestampedWindowStore<Long, CountWrapper>) context
				.getStateStore(GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR.storeName());

		// punctuate scheduled every window size + grace size minutes
		context.schedule(Duration.ofMinutes(15), PunctuationType.STREAM_TIME, timestamp -> { // TODO try wall clock time

			log.info("-- Punctuate Time: {}, {}, task {}", Instant.ofEpochMilli(timestamp), timestamp,
					context.taskId());

			Instant currentQuarter = GeoDataUtils.inferCurrentQuarterHourStartTime(timestamp);

			log.info("Current quarter: {} - {}", currentQuarter, currentQuarter.toEpochMilli());

			KeyValueIterator<Windowed<Long>, ValueAndTimestamp<CountWrapper>> iterator = this.store.all();

			while (iterator.hasNext()) {

				KeyValue<Windowed<Long>, ValueAndTimestamp<CountWrapper>> kv = iterator.next();

				if (kv.value.value().isFinalized() == false && kv.key.window().startTime().isBefore(currentQuarter)) {

					log.info("{} ({}): {}", kv.key.key(), kv.key.window().startTime(), kv.value.value());

					context.forward(kv.key.key(), kv.value.value().getCount());

					context.commit();

					kv.value.value().finalize();

					this.store.put(kv.key.key(), kv.value, kv.key.window().start());

				}

			}

			iterator.close();

		});

	}

	@Override
	public KeyValue<Long, Long> transform(Windowed<Long> key, CountWrapper value) {
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
