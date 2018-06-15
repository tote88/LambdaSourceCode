package bdma.labos.lambda;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import bdma.labos.lambda.exercises.Exercise1;
import bdma.labos.lambda.exercises.Exercise1_speed;
import bdma.labos.lambda.exercises.Exercise2_batch;
import bdma.labos.lambda.exercises.Exercise3_serving;

public class Lambda {

	// IMPORTANT: modify to your bdma user: e.g., bdma00
	private static String TWITTER_CONFIG_PATH = "/home/bdma32/LambdaSourceCode/src/main/resources/twitter.txt";
	
	public static void main(String[] args) {
		
	
		LogManager.getRootLogger().setLevel(Level.ERROR);
		try {
			if (args[0].equals("-exercise1")) {
				Exercise1.run(TWITTER_CONFIG_PATH);
			} else if (args[0].equals("-exercise1_speed")) {
				Exercise1_speed.run(TWITTER_CONFIG_PATH);
			} else if (args[0].equals("-exercise2_batch")) {
				Exercise2_batch.run();
			} else if (args[0].equals("-exercise3_serving")) {
				Exercise3_serving.run(args[1]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
