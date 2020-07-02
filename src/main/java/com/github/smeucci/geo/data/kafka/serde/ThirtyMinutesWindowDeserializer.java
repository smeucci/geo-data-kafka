package com.github.smeucci.geo.data.kafka.serde;

import java.time.Duration;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

public class ThirtyMinutesWindowDeserializer implements Deserializer<Windowed<String>> {

	@Override
	public Windowed<String> deserialize(String topic, byte[] data) {

		Windowed<String> windowedKey = WindowedSerdes
				.timeWindowedSerdeFrom(String.class, Duration.ofMinutes(30).toMillis()).deserializer()
				.deserialize(topic, data);

		return windowedKey;
	}

}
