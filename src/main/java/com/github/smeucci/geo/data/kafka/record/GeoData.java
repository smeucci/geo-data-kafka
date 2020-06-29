package com.github.smeucci.geo.data.kafka.record;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

public record GeoData(long id, long timestamp, double latitude, double longitude) {

	public static GeoData generate() {

		long id = randomId();

		long timestamp = Instant.now().toEpochMilli();

		double latitude = (Math.random() * 180) - 90;

		double longitude = (Math.random() * 360) - 180;

		return new GeoData(id, timestamp, latitude, longitude);

	}

	public static GeoData generateNorthen() {

		long id = randomId();

		long timestamp = Instant.now().toEpochMilli();

		double latitude = (Math.random() * (-90)) + 90;

		double longitude = (Math.random() * 360) - 180;

		return new GeoData(id, timestamp, latitude, longitude);

	}

	public static GeoData generateSouthern() {

		long id = randomId();

		long timestamp = Instant.now().toEpochMilli();

		double latitude = (Math.random() * 90) - 90;

		double longitude = (Math.random() * 360) - 180;

		return new GeoData(id, timestamp, latitude, longitude);

	}

	private static long randomId() {

		return ThreadLocalRandom.current().nextLong(1, 30 + 1);

	}

}
