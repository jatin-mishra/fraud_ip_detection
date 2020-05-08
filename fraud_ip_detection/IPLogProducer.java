// it is designed to be like a real-time log producer where producer
// runs every three seconds and produces a new record with random IP 
// address

import com.packt.reader.PropertyReader;
import org.apache.kafka.clients.producer.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.Future;

public class IPLogProducer extends TimerTask{
	static String path = "";

	public BufferedReader readFile(){
		BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/IP_LOG.log")));
		return br;
	}

	public static void main(String[] args){
		Timer timer = new Timer();
		timer.schedule(new IPLogProducer(), 3000, 3000);
	}

	private String getNewRecordWithRandomIP(String line){
		Random r = new Random();
		String ip = r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
		String[] columns = line.split(" ");
		columns[0] = ip;
		return Arrays.toString(columns);
	}

	@Override
	public void run(){
		PropertyReader propertyReader = new PropertyReader();

		Properties produceProps = new Properties();
		produceProps.put("bootstrap.servers",propertyReader.getPropertyValue("broker.list"));
		produceProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		produceProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		produceProps.put("auto.create.topics.enable",true);


		KafkaProducer<String,String> ipProducer = new KafkaProducer<String,String>(produceProps);
		BufferedReader br = readFile();
		String oldLine = "";
		try{
			while((oldLine = br.readLine()) != null){
				String line = getNewRecordWithRandomIP(oldLine).replace("[","").replace("]","");
				ProducerRecord ipData = new ProducerRecord<String,String>(propertyReader.getPropertyValue("topic"),line);
				Future<RecordMetadata> recordmetadata = ipProducer.send(ipData);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			ipProducer.close();
		}
	}
}