package com.github.smeucci.geo.data.kafka.converter;

import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;

public abstract class JsonConverter<T> {

	protected Logger log = LoggerFactory.getLogger(this.getClass());

	private Moshi moshi = new Moshi.Builder().build();

	private JsonAdapter<T> adapter;

	public abstract Class<T> getClassOfType();

	public String toJson(T object) {

		JsonAdapter<T> jsonAdapter = getJsonAdapter(getClassOfType());

		return jsonAdapter.toJson(object);

	}

	public Optional<T> fromJson(String json) {

		JsonAdapter<T> jsonAdapter = getJsonAdapter(getClassOfType());

		try {

			return Optional.of(jsonAdapter.fromJson(json));

		} catch (IOException e) {

			log.error("Could not parse json string in to object of type {}", getClassOfType());

			return Optional.empty();

		}

	}

	private JsonAdapter<T> getJsonAdapter(Class<T> type) {

		if (this.adapter == null) {
			this.adapter = this.moshi.adapter(type);
		}

		return adapter;

	}

}
