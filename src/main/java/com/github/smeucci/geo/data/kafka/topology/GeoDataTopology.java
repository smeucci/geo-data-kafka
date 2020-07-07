package com.github.smeucci.geo.data.kafka.topology;

import java.util.function.Consumer;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import com.github.smeucci.geo.data.kafka.config.GeoDataConfig;

public class GeoDataTopology {

	private final KStream<Long, String> sourceStream;

	private final StreamsBuilder streamsBuilder;

	public GeoDataTopology(final GeoDataConfig.Topic topic) {

		streamsBuilder = new StreamsBuilder();

		sourceStream = streamsBuilder.stream(topic.topicName(),
				Consumed.as(GeoDataConfig.Operator.SOURCE_GEO_DATA.operatorName()));

	}

	public GeoDataTopology addOperator(final Consumer<KStream<Long, String>> processor) {

		processor.accept(sourceStream);

		return this;

	}

	public Topology build() {

		return streamsBuilder.build();

	}

}
