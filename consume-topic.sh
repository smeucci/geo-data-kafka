echo $1 $2

topic=$1
value=${2-"string"}

if [ $value == "long" ]
then

	kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $topic \
		--property print.key=true \
		--property print.value=true \
		--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

else
	
	kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $topic \
		--property print.key=true \
		--property print.value=true \
	
fi