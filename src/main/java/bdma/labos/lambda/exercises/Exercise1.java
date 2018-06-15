package bdma.labos.lambda.exercises;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import bdma.labos.lambda.utils.Utils;

public class Exercise1 {

	@SuppressWarnings("serial")
	public static void run(String twitterFile) throws Exception {
		SparkConf conf = new SparkConf().setAppName("LambdaArchitecture").setMaster("spark://master:7077");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaStreamingContext streamContext = new JavaStreamingContext(context, new Duration(1000));
		
		JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = Utils.getKafkaStream(streamContext, twitterFile);
		/*********************/
		//insert your code here 
		JavaDStream<String> statuses = kafkaStream.map(stringStringConsumerRecord -> stringStringConsumerRecord.value());
		statuses.print();
		
		/********************/
		
		streamContext.start();
		streamContext.awaitTermination();
	}
	
}
