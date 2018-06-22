package bdma.labos.lambda.exercises;


import bdma.labos.lambda.writers.WriterClient;
import bdma.labos.lambda.writers.WriterServer;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import org.bson.Document;

public class Exercise3_serving {

	public static void run(String hashtag) throws Exception {
								
		/*********************/
		//insert your code here 

		MongoClient client = new MongoClient();
		MongoDatabase database = client.getDatabase("twitter");
		MongoCollection<Document> sentimentCollection = database.getCollection("twitter_sentiment");
		MongoCollection<Document> summaryCollection = database.getCollection("twitter_summary");

		FindIterable<Document> sentimentDocuments = sentimentCollection.find();
		FindIterable<Document> summaryDocuments = summaryCollection.find();

		for (Document doc : sentimentDocuments) {
			System.out.println("S " + doc.toJson());
		}

		for (Document doc : summaryDocuments) {
			System.out.println("W " + doc.toJson());
		}



		/*********************/
		
	}
	
}
