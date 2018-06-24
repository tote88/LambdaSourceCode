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

		Document query = new Document();
		Document eqHashtag = new Document();
		eqHashtag.put("$eq", hashtag);
		query.put("hashtag", eqHashtag);

		FindIterable<Document> hashtagSentiment = sentimentCollection.find(query);

		for (Document doc : hashtagSentiment) {
			System.out.println(doc.toJson());

			Document queryDoc = new Document();
			Document eqId = new Document();
			eqId.put("$eq", doc.get("id"));
			queryDoc.put("id", eqId);
			FindIterable<Document> hashtagTweets = summaryCollection.find(queryDoc);
			for (Document doc2 : hashtagTweets) {
				System.out.println(doc2.toJson());
			}
			System.out.println("=======================================");
		}
/*
		FindIterable<Document> results = summaryCollection.find();

		for (Document doc : results) {
			System.out.println(getTwitterDate(doc.getString("time")));
		}

		results = sentimentCollection.find();

		for (Document doc : results) {
			System.out.println("W " + doc.toJson());
		}
*/



		/*********************/
		
	}
	
}
