package com.github.smeucci.geo.data.kafka.record;

import java.time.Instant;

public record GeoData(long timestamp, double latitude, double longitude) {

	public static GeoData generate() {

		long timestamp = Instant.now().toEpochMilli();

		double latitude = (Math.random() * 180) - 90;

		double longitude = (Math.random() * 360) - 180;

		return new GeoData(timestamp, latitude, longitude);

	}

	public static GeoData generateNorthen() {

		long timestamp = Instant.now().toEpochMilli();

		double latitude = (Math.random() * (-90)) + 90;

		double longitude = (Math.random() * 360) - 180;

		return new GeoData(timestamp, latitude, longitude);

	}

	public static GeoData generateSouthern() {

		long timestamp = Instant.now().toEpochMilli();

		double latitude = (Math.random() * 90) - 90;

		double longitude = (Math.random() * 360) - 180;

		return new GeoData(timestamp, latitude, longitude);

	}

}
