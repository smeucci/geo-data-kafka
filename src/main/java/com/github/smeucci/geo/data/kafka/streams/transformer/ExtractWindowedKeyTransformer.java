package com.github.smeucci.geo.data.kafka.streams.transformer;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;
import com.github.smeucci.geo.data.kafka.utils.GeoDataUtils;

public class ExtractWindowedKeyTransformer implements Transformer<Windowed<Long>, Long, KeyValue<Long, Long>> {

	private static final Logger log = LoggerFactory.getLogger(ExtractWindowedKeyTransformer.class);

	private TimestampedWindowStore<Long, Long> store;

	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {

		// get the window state store
		this.store = (TimestampedWindowStore<Long, Long>) context
				.getStateStore(GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR.storeName());

		// punctuate scheduled every window size + grace size minutes
		context.schedule(Duration.ofMinutes(15), PunctuationType.STREAM_TIME, timestamp -> {

			Instant start = GeoDataUtils.inferQuarterHourStartTimeFromPuctuate(timestamp, Duration.ZERO);

			log.info("Retrieving Final Results for Window: [{}, {}]", start, start.plus(15, ChronoUnit.MINUTES));

			ValueAndTimestamp<Long> countWrapper = this.store.fetch(start.toEpochMilli(), start.toEpochMilli());

			if (countWrapper != null && countWrapper.value() != null) {

				context.forward(start.toEpochMilli(), countWrapper.value());

			}

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
