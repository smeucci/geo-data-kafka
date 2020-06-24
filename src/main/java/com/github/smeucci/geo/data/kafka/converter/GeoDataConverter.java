package com.github.smeucci.geo.data.kafka.converter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.smeucci.geo.data.kafka.record.GeoData;

public class GeoDataConverter extends JsonConverter<GeoData> {

	protected Logger log = LoggerFactory.getLogger(this.getClass());

	@Override
	public Class<GeoData> getClassOfType() {
		return GeoData.class;
	}

}
