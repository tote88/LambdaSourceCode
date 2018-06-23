package bdma.labos.lambda.exercises;

import bdma.labos.lambda.writers.WriterClient;
import com.mongodb.spark.MongoSpark;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import bdma.labos.lambda.utils.Utils;
import bdma.labos.lambda.writers.WriterServer;
import org.bson.Document;
import twitter4j.JSONObject;
import twitter4j.Status;

public class Exercise1_speed {

	@SuppressWarnings("serial")
	public static void run(String twitterFile) throws Exception {
		// Creating and starting up the writer server for HDFS
		WriterServer writerServer = new WriterServer();
		writerServer.start();						
		
		//Including MongoDB connector info into Spark Session configuration (twitter: MongoDB database;   twitter_summary: MongoDB collection)
		// see more info at: https://docs.mongodb.com/spark-connector/master/java-api/
		SparkSession spark = SparkSession.builder()
			      .master("spark://master:7077")
			      .appName("LambdaArchitecture")
			      .config("spark.mongodb.input.uri", "mongodb://master:27017/twitter.twitter_summary")
			      .config("spark.mongodb.output.uri", "mongodb://master:27017/twitter.twitter_summary")
			      .getOrCreate();					
		
		JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
		 
		// Setting up the Spark streaming context and a batch duration (sliding window) 
		JavaStreamingContext streamContext = new JavaStreamingContext(context, new Duration(1000));
		
		// Creating a Kafka stream for twitter feeds 
		JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = Utils.getKafkaStream(streamContext, twitterFile);
		
		/*********************/					
		// insert your code here


		WriterClient writerClient = new WriterClient();

		JavaDStream<String> statuses = kafkaStream.map(stringStringConsumerRecord -> stringStringConsumerRecord.value());

		statuses.print();

		statuses.foreachRDD(statusRDD -> {
			for(String status : statusRDD.collect()) {
				writerClient.write((status+"\n").getBytes());
			}
		});

		JavaDStream<JSONObject> jsonObjects = statuses.map(s -> {
			JSONObject jsonObject = new JSONObject(s);
			return jsonObject;
		});

		JavaDStream<Document> documentMap = jsonObjects.map(s -> {
			Document doc = new Document();
			doc.put("id", s.get("id"));
			String text = s.get("text").toString();
			String[] split = text.split(" ");
			String hashtag = new String("");
			for (String st : split) {
				if (st.startsWith("#")) {
					hashtag = st;
					break;
				}
			}
			doc.put("text", text);
			doc.put("hashtag", hashtag);
			doc.put("time", s.get("created"));
			return doc;
		});
		documentMap.foreachRDD((v1, v2) -> MongoSpark.save(v1));

		/*********************/

		streamContext.start();
		streamContext.awaitTermination();
		
		writerServer.finish();
	}
	
}
