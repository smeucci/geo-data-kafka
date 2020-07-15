package com.github.smeucci.geo.data.kafka.converter;

import com.github.smeucci.geo.data.kafka.record.CountWrapper;

public class CountWrapperConverter extends JsonConverter<CountWrapper> {

	@Override
	public Class<CountWrapper> getClassOfType() {
		return CountWrapper.class;
	}

}
