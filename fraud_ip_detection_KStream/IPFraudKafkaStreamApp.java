package com.packt.Kafka;

import com.packt.Kafka.lookup.CacheIPLookup;
import com.packt.Kafka.utils.PropertyReader;
import org.apache.kafka.serialization.Serde;
import org.apache.kafka.serialization.Serdes;
import org.apache.kafka.Streams.KafkaStreams;
import org.apache.kafka.Streams.StreamsConfig;
import org.apache.kafka.Streams.kStream.KStream;
import org.apache.kafka.Streams.kStream.KStreamBuilder;

import java.util.Properties;

public class IPFraudKafkaStreamApp{

	private static CacheIPLookup cacheIpLookup = new CacheIPLookup();
	private static PropertyReader propertyReader = new PropertyReader();


	public static void main(String[] args){
		Properties kafkaStreamProperties = new Properties();


		// application_id_config
		kafkaStreamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG,"ipDetectionKStream");

		// bootstrap_servers_config
		kafkaStreamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		
		// zookeeper_connect_config
		kafkaStreamProperties.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,"localhost:2181");

		// key_serde_class_config
		kafkaStreamProperties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());

		// value_Serde_class_config
		kafkaStreamProperties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());

		Serde<String> stringSerde = Serdes.String();

		KStreamBuilder streamTopology = kew KStreamBuilder();

		KStream<String,String> records = streamTopology.Stream(stringSerde,stringSerde,PropertyReader.getPropertyValue("topic"));

		KStream<String,String> fraudIpRecords = records.filter((k,v) -> isFraud(v));

		fraudIpRecords.to(PropertyReader.getPropertyValue("output_topic"));

		KafkaStreams streamManager = new KafkaStreams(streamTopology,kafkaStreamProperties);
		streamManager.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streamManager::close));

	}


	private static boolean isFraud(string record){
		String ip = record.split(" ")[0];

		String[] ranges = ip.split("\.");

		String range = null;
		try{
			range = ranges[0];
		}catch(ArrayIndexOutOfBoundsException ex){
			ex.printStackTrace();
		}

		return cacheIpLookup.isFraudIP(range);
	}

}