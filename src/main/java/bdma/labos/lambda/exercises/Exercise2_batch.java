package bdma.labos.lambda.exercises;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.SparkSession;


public class Exercise2_batch {

	// IMPORTANT: modify to your bdma user: e.g., bdma00
	private static String HDFS = "hdfs://master:27000/user/bdma32";
	
	
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
