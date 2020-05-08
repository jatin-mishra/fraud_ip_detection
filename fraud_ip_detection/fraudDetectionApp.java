import com.packt.reader.PropertyReader;
import org.apache.Spark.SparkConf;
import org.apache.Spark.api.java.function.Function;
import org.apache.Spark.streaming.api.java.JavaStreamingContext;

import java.util.*;
import scala.Tuple2;
import kafka.serializer.StringDecoder;
import org.apache.Spark.streaming.api.java.*;
import org.apache.Spark.streaming.kafka.KafkaUtils;
import org.apache.Spark.streaming.Durations;

public class fraudDetectionApp{
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception{
		PropertyReader propertyReader = new PropertyReader();
		CacheIpLookup lookup = new CacheIpLookup();

		SparkConf sparkconf = new SparkConf().setAppName("IP_FRAUD");
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkconf,Durations.seconds(3));

		Set<String> topicSet = new HashSet<>(Arrays.asList(propertyReader.getPropertyValue("topic").split(",")));
		Map<String,String> kafkaConfiguration = new HashMap<>();
		kafkaConfiguration.put("metadata.broker.list",propertyReader.getPropertyValue("broker.list"));
		kafkaConfiguration.put("group.id",propertyReader.getPropertyValue("group.id"));

		JavaPairInputDStream<String,String> messages = KafkaUtils.createDirectStream(javaStreamingContext,String.class,String.class,StringDecoder.class,StringDecoder.class,kafkaConfiguration,topicSet);
		JavaDStream<String> ipRecords = messages.map(Tuple2::_2);
		JavaDStream<String> fraudIps = ipRecords.filter(new Function<String,Boolean>(){

			@Override
			public Boolean call(String s) throws Exception{
				String IP = s.split(",")[0];
				String[] ranges = IP.split("\\.");
				String range=null;

				try{
					range = ranges[0];
				}catch(ArrayIndexOutOfBoundsException ex){

				}

				return lookup.isFraudIP(range);
			}
		});

		fraudIPs.dstream().saveAsTextFiles("hdfs://localhost:9000/user/packt/streaming/fraudips","");
		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();
	} 
}