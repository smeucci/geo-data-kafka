package com.github.smeucci.geo.data.kafka.streams.processor;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;

public class AggregateQuarterHourCountByWindowProcessor implements Processor<Windowed<Long>, Long> {

	private static final Logger log = LoggerFactory.getLogger(AggregateQuarterHourCountByWindowProcessor.class);

	private TimestampedWindowStore<Long, Long> store;

	private Map<Long, Long> windowMap = new TreeMap<>();

	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {

		this.store = (TimestampedWindowStore<Long, Long>) context
				.getStateStore(GeoDataConfig.Store.COUNT_EVERY_QUARTES_HOUR_BY_ID.storeName());

		context.schedule(Duration.ofMinutes(15), PunctuationType.STREAM_TIME, timestamp -> {

			log.info("Punctuate Time: {}", timestamp);

			ZonedDateTime punctuateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);

			int punctuateQuarter = punctuateTime.getMinute() - (punctuateTime.getMinute() % 15);

			log.info("Quarter: {}", punctuateQuarter);

			Instant from = punctuateTime.withMinute(punctuateQuarter).withSecond(0).withNano(0).toInstant().minus(15,
					ChronoUnit.MINUTES);
			Instant to = from;

			log.info("Search Window: [{}, {}]", from, to);

			KeyValueIterator<Windowed<Long>, ValueAndTimestamp<Long>> iterator = this.store.fetchAll(from, to);

			while (iterator.hasNext()) {

				KeyValue<Windowed<Long>, ValueAndTimestamp<Long>> kv = iterator.next();

				log.info("{}", kv);

				Long total = windowMap.get(kv.key.window().start());

				total = total == null ? kv.value.value() : total + kv.value.value();

				windowMap.put(kv.key.window().start(), total);

			}

			iterator.close();

			log.info("STORE count");
			windowMap.entrySet().forEach(e -> log.info("{}", e));

			// forward to topic
			Long total = windowMap.get(from.toEpochMilli());

			if (total != null) {
				context.forward(from.toEpochMilli(), total);
			}

		});

	}

	@Override
	public void process(Windowed<Long> key, Long value) {

		log.info("PROCESS");

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

		log.info("CLOSE");

	}

}
