package eu.spaziodati.batchrefine.spark;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import eu.spaziodati.batchrefine.spark.http.SparkRefineHTTPClient;

public class RefineOnSpark {

	/**
	 * This is a simple Spark Driver program, aimed to connect to the Spark
	 * cluster, load and partition the textFile and invoke applyOperations on
	 * it.
	 * 
	 * Each worker node submits the transform job to locally running OpenRefine
	 * Instance, using {@link SparkRefineHTTPClient}
	 * 
	 */

	public static void main(String[] args) throws URISyntaxException,
			IOException {

		if (args.length < 2) {
			System.err
					.println("Usage: textFile operations [spark://MASTER_IP:PORT] default: local");
			System.exit(1);
		}

		String sparkMaster = (args.length == 3) ? args[2] : "local";

		SparkConf sparkConf = new SparkConf()
				.setAppName("SparkRefineClient")
				.setMaster(sparkMaster)
				.set("spark.executor.memory", "600m")
				.set("spark.executor.extraClassPath",
						"/home/andrey/Software/spark-1.0.0/work/executorlib/RefineOnSpark-0.1.jar:/home/andrey/Software/spark-1.0.0/work/executorlib/*")
						.set("spark.speculation", "false");

		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		sparkContext.addFile(args[1]);

		JavaRDD<String> text = sparkContext.textFile(args[0]);

		// make header variable available to all the Workers.

		Broadcast<String> header = sparkContext.broadcast(text.first());

		JavaRDD<String> temp = text.mapPartitions(new JobFunction(header,
				args[1]));

		temp.cache();
		List<String> output = temp.collect();
		// output.add(0, header.getValue());
		output.add(0, header.getValue());

		FileUtils.writeLines(new File("output_" + args[0]), output);

		sparkContext.stop();

		System.out.println("Done! output count is: " + output.size());
	}
}
