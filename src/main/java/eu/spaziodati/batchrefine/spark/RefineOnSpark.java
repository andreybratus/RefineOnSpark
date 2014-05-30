package eu.spaziodati.batchrefine.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


public class RefineOnSpark {

	public static void main(String[] args) {

		if (args.length < 2) {
			System.err.println("Usage: ");

		}

		SparkConf sparkConf = new SparkConf().setAppName("BatchRefine")
				.setMaster(args[0]);
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		
	}

}
