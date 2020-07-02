echo $1 $2 $3

topic=$1
key=${2-"string"}
value=${3-"string"}


if [ $key == "long" ]
then
	keydes=org.apache.kafka.common.serialization.LongDeserializer
else
	keydes=org.apache.kafka.common.serialization.StringDeserializer
fi

if [ $value == "long" ]
then
	valuedes="org.apache.kafka.common.serialization.LongDeserializer"
else
	valuedes="org.apache.kafka.common.serialization.StringDeserializer"
fi

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $topic \
	--property print.key=true \
	--property print.value=true \
	--property key.deserializer=$keydes \
	--property value.deserializer=$valuedes