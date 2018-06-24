package bdma.labos.lambda.exercises;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.bson.Document;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import twitter4j.Status;

import java.util.Arrays;
import java.util.List;
import java.util.Set;


public class Exercise2_batch {

	// IMPORTANT: modify to your bdma user: e.g., bdma00
	private static String HDFS = "hdfs://master:27000/user/bdma32";

	public static JavaRDD<Document> sentimentAnalysis(JavaMongoRDD<Document> statuses) {
		printRDD(statuses);
		JavaRDD<Document> englishStatuses = statuses
				.filter(status -> LanguageDetector.isEnglish(status.get("text").toString()));

		JavaRDD<Document> englishNotNullStatuses = englishStatuses
				.filter(status -> status.get("text") != null);

		JavaPairRDD<Long, String> tuples = englishNotNullStatuses.
				mapToPair(status -> new Tuple2<Long, String>((Long)status.get("id"),
						status.get("text").toString().replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase()));

		JavaPairRDD<Long, String> noStopWords = tuples
				.mapToPair(longStringTuple2 -> {
					List<String> split = Arrays.asList(longStringTuple2._2.split(" "));
					String text = "";
					List<String> stopWords = StopWords.getWords();
					for (String word : split){
						if (!stopWords.contains(word)) text = text.concat(word + " ");
					}
					return new Tuple2<Long, String>(longStringTuple2._1, text);
				});


		JavaPairRDD<Long, Tuple3<String, Double, Double>> tweetsScored = noStopWords
				.mapToPair(longStringTuple2 -> {
					int posWord = 0;
					int negWord = 0;
					int words = 0;
					List<String> split = Arrays.asList(longStringTuple2._2.split(" "));
					Set<String> positiveWords = PositiveWords.getWords();
					Set<String> negativeWords = NegativeWords.getWords();
					for (String word : split){
						if (positiveWords.contains(word)) posWord++;
						if (negativeWords.contains(word)) negWord++;
						words++;
					}
					double positive = (double) posWord / words;
					double negative = (double) negWord / words;
					Tuple3<String, Double, Double> t = new Tuple3(longStringTuple2._2, positive, negative);
					return new Tuple2<Long, Tuple3<String, Double, Double>>(longStringTuple2._1, t);
				});

		//tweetsScored.print();
		JavaPairRDD<Long, Tuple3<String, Double, Double>> nonNeutralTweets = tweetsScored
				.filter(longTuple3Tuple2 -> longTuple3Tuple2._2._2() > 0.0 || longTuple3Tuple2._2._3() > 0.0);

		//nonNeutralTweets.print();
		JavaRDD<Document> tweets = nonNeutralTweets
				.map(longTuple3Tuple2 -> {
					String dict = "";
					if (longTuple3Tuple2._2._2() > longTuple3Tuple2._2._3()) dict = "Positive";
					else if (longTuple3Tuple2._2._3() > longTuple3Tuple2._2._2()) dict = "Negative";
					else dict = "Neutral";
					Document doc = new Document();
					doc.put("id", longTuple3Tuple2._1);
					doc.put("text", longTuple3Tuple2._2._1());
					doc.put("sentiment", dict);
//					Tuple4<String, Double, Double, String> t = new Tuple4<>(longTuple3Tuple2._2._1(), longTuple3Tuple2._2._2(), longTuple3Tuple2._2._3(), dict);
					return doc;
				});

		return tweets;
	}
	
	@SuppressWarnings("serial")
	public static void run() throws Exception {
		
		//Including MongoDB connector info into Spark Session configuration (twitter: MongoDB database;   twitter_sentiment: MongoDB collection) 
		// see more info at: https://docs.mongodb.com/spark-connector/master/java-api/
		SparkSession spark = SparkSession.builder()
			      .master("spark://master:7077")
			      .appName("LambdaArchitecture")
			      .config("spark.mongodb.input.uri", "mongodb://master:27017/twitter.twitter_summary")
			      .config("spark.mongodb.output.uri", "mongodb://master:27017/twitter.twitter_sentiment")
			      .getOrCreate();
		 
		 
		JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
		
		/*********************/		
		//insert your code here

		JavaMongoRDD<Document> rdd = MongoSpark.load(context);
        System.out.println("LOADED");
		JavaRDD<Document> documentJavaRDD = sentimentAnalysis(rdd);
        System.out.println("SENTIMDONE");

		printRDD(documentJavaRDD);

		MongoSpark.save(documentJavaRDD);

		/*********************/
        
        context.close();
	}

	private static void printRDD(JavaRDD<Document> documentJavaRDD) {
		for (Document doc : documentJavaRDD.collect()) {
			System.out.println(doc.toJson());
		}
	}
        
        
}
