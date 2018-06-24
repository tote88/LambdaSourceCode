package bdma.labos.lambda.exercises;


import bdma.labos.lambda.writers.WriterClient;
import bdma.labos.lambda.writers.WriterServer;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import org.bson.Document;
import org.joda.time.DateTime;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Exercise3_serving {


	public static Date getTwitterDate(String date) throws ParseException
	{
//		final String TWITTER = "EEE dd MMM yyyy HH:mm:ss Z";
		final String TWITTER = "EEE MMM dd HHmmss ZZZZZ yyyy";
		SimpleDateFormat sf = new SimpleDateFormat(TWITTER, Locale.ENGLISH);
		sf.setLenient(true);
		return sf.parse(date);
	}

	public static void run(String hashtag) throws Exception {
								
		/*********************/
		//insert your code here 

		MongoClient client = new MongoClient();
		MongoDatabase database = client.getDatabase("twitter");
        MongoCollection<Document> summaryCollection = database.getCollection("twitter_summary");
        MongoCollection<Document> sentimentCollection = database.getCollection("twitter_sentiment");

		FindIterable<Document> results = summaryCollection.find();

		for (Document doc : results) {
			System.out.println(getTwitterDate(doc.getString("time")));
		}

		results = sentimentCollection.find();

		for (Document doc : results) {
			System.out.println("W " + doc.toJson());
		}



		/*********************/
		
	}
	
}
