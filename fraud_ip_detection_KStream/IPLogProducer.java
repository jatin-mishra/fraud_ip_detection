package com.packt.Kafka.producer;

import com.packt.Kafka.utils.PropertyReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.*;
import java.util.concurrent.Future;

public class IPLogProducer extends TimerTask{

	public BufferedReader readFile(){
		BufferedReader reader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/IP_Log.log")));
		return reader;
	}


	public static void main(final String[] args){
		Timer timer = new Timer();
		timer.schedule(new IPLogProducer(),3000,3000);
	}


	private String getNewRecordWithRandomIp(String line){
		Random random = new Random();
		String ip = r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
		String columns = line.split(" ");
		columns[0] = ip;
		return Arrays.toString(columns);
	}


	@Override
	public void run(){
		PropertyReader reader = new PropertyReader();

		Properties properties = new Properties();

		// bootstrap.servers
		properties.put("bootstrap.servers","localhost:9092");
		// key serializer
		properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		// value serializer
		properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		// auto.create.topic.enable
		properties.put("auto.create.topic.enable","true");

		KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

		BufferedReader br = readFile();
		String oldLine = "";

		try{
			while((oldLine = br.readLine()) != null){
				String line = getNewRecordWithRandomIp(oldLine).replace("[","").replace("]","");

				ProducerRecord ipData = new ProducerRecord<String,String>(PropertyReader.getPropertyValue("topic"),line);
				RecordMetadata metadata = producer.send(ipData).get();
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		producer.close();
	}
}