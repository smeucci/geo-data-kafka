package com.github.smeucci.geo.data.kafka.streams.transformer;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

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

public class AggregateQuarterHourByWindowTransformer
		implements Transformer<Windowed<Long>, Long, KeyValue<Long, Long>> {

	private static final Logger log = LoggerFactory.getLogger(AggregateQuarterHourByWindowTransformer.class);

	private TimestampedWindowStore<Long, Long> store;

	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {

		this.store = (TimestampedWindowStore<Long, Long>) context
				.getStateStore(GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR_BY_ID.storeName());

		context.schedule(Duration.ofMinutes(15), PunctuationType.STREAM_TIME, timestamp -> {

			ZonedDateTime closeWindowTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC)
					.minusMinutes(15);

			int closeWindowQuarter = closeWindowTime.getMinute() - (closeWindowTime.getMinute() % 15);

			Instant from = closeWindowTime.withMinute(closeWindowQuarter).withSecond(0).withNano(0).toInstant();

			log.info("Aggregating Results for Window: [{}, {}]", from, from.plus(15, ChronoUnit.MINUTES));

			KeyValueIterator<Windowed<Long>, ValueAndTimestamp<Long>> iterator = this.store.fetchAll(from, from);

			Long total = 0l;

			while (iterator.hasNext()) {

				total += iterator.next().value.value();

			}

			iterator.close();

			// forward downstream
			if (total != null) {
				context.forward(from.toEpochMilli(), total);
			}

		});

	}

	@Override
	public KeyValue<Long, Long> transform(Windowed<Long> key, Long value) {
		// Not needed
		return null;
	}

	@Override
	public void close() {
		// Not needed
	}

}
