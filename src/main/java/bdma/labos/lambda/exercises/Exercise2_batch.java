package bdma.labos.lambda.exercises;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
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

	public static JavaPairDStream<Long, Tuple4<String, Double, Double, String>> sentimentAnalysis(JavaDStream<Status> statuses) {
		JavaDStream<Status> englishStatuses = statuses
				.filter(status -> LanguageDetector.isEnglish(status.getText()));

		JavaDStream<Status> englishNotNullStatuses = englishStatuses
				.filter(status -> status.getText() != null);

		JavaPairDStream<Long, String> tuples = englishNotNullStatuses.
				mapToPair(status -> new Tuple2<Long, String>(status.getId(),
						status.getText().replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase()));

		JavaPairDStream<Long, String> noStopWords = tuples
				.mapToPair(longStringTuple2 -> {
					List<String> split = Arrays.asList(longStringTuple2._2.split(" "));
					String text = "";
					List<String> stopWords = StopWords.getWords();
					for (String word : split){
						if (!stopWords.contains(word)) text = text.concat(word + " ");
					}
					return new Tuple2<Long, String>(longStringTuple2._1, text);
				});


		JavaPairDStream<Long, Tuple3<String, Double, Double>> tweetsScored = noStopWords
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
		JavaPairDStream<Long, Tuple3<String, Double, Double>> nonNeutralTweets = tweetsScored
				.filter(longTuple3Tuple2 -> longTuple3Tuple2._2._2() > 0.0 || longTuple3Tuple2._2._3() > 0.0);

		//nonNeutralTweets.print();
		JavaPairDStream<Long, Tuple4<String, Double, Double, String>> tweets = nonNeutralTweets
				.mapToPair(longTuple3Tuple2 -> {
					String dict = "";
					if (longTuple3Tuple2._2._2() > longTuple3Tuple2._2._3()) dict = "Positive";
					else if (longTuple3Tuple2._2._3() > longTuple3Tuple2._2._2()) dict = "Negative";
					else dict = "Neutral";
					Tuple4<String, Double, Double, String> t = new Tuple4<>(longTuple3Tuple2._2._1(), longTuple3Tuple2._2._2(), longTuple3Tuple2._2._3(), dict);
					return new Tuple2<Long, Tuple4<String, Double, Double, String>>(longTuple3Tuple2._1, t);
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
			      .config("spark.mongodb.input.uri", "mongodb://master:27017/twitter.twitter_sentiment")
			      .config("spark.mongodb.output.uri", "mongodb://master:27017/twitter.twitter_sentiment")
			      .getOrCreate();
		 
		 
		JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
		
		/*********************/		
		//insert your code here


		
        
        /*********************/
        
        context.close();
	}
        
        
}
