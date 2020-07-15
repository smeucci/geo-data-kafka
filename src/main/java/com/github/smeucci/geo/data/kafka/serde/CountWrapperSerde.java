package com.github.smeucci.geo.data.kafka.serde;

import java.nio.charset.Charset;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.github.smeucci.geo.data.kafka.converter.CountWrapperConverter;
import com.github.smeucci.geo.data.kafka.record.CountWrapper;

public class CountWrapperSerde implements Serializer<CountWrapper>, Deserializer<CountWrapper> {

	@Override
	public byte[] serialize(String topic, CountWrapper data) {

		byte[] bytes = null;

		if (data != null) {

			CountWrapperConverter converter = new CountWrapperConverter();

			String json = converter.toJson(data);

			bytes = json.getBytes(Charset.forName("UTF-8"));

		}

		return bytes;

	}

	@Override
	public CountWrapper deserialize(String topic, byte[] data) {

		CountWrapper countWrapper = null;

		if (data != null && data.length > 0) {

			String json = new String(data, Charset.forName("UTF-8"));

			CountWrapperConverter converter = new CountWrapperConverter();

			countWrapper = converter.fromJson(json).get();

		}

		return countWrapper;

	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		Deserializer.super.configure(configs, isKey);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		Deserializer.super.close();
	}

}
