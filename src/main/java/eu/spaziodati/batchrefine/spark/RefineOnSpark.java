package eu.spaziodati.batchrefine.spark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import eu.spaziodati.batchrefine.spark.http.SparkRefineHTTPClient;
import eu.spaziodati.batchrefine.spark.utils.DriverOptions;
import eu.spaziodati.batchrefine.spark.utils.JobOptions;

public class RefineOnSpark {

	private static JavaSparkContext sparkContext;

	/**
	 * This is a simple Spark Driver CLI program, aimed to connect to the Spark
	 * Cluster, with specified options in the {@link DriverOptions}. After
	 * connecting to the master cluster, brings to the CLI, where you can issue
	 * requests for transforming data using options {@link JobOptions}
	 * 
	 * Each worker node submits the transform job to locally running OpenRefine
	 * Instance, using {@link SparkRefineHTTPClient}
	 * 
	 * INPUTFILE is supposed to be present on all worker nodes under the same
	 * location. TRANSFORMFILE will be shipped to workers using HTTP fileserver.
	 * 
	 * @author andrey
	 */

	public static void main(String[] args) {

		DriverOptions options = new DriverOptions();

		CmdLineParser parser = new CmdLineParser(options);
		try {
			parser.parseArgument(args);

			if (options.getArguments().size() < 1) {
				printUsage(parser);
				System.exit(-1);
			}

			SparkConf sparkConfiguration = new SparkConf(true)
					.setMaster(options.getArguments().get(0))
					.setAppName(options.getAppName())
					.set("spark.executor.memory", options.getExecutorMemory())
					.set("spark.executor.extraClassPath",
							"/shared/home-05/mega/RefineOnSpark/RefineOnSpark-0.1.jar:/shared/home-05/mega/RefineOnSpark/lib/*");

			sparkContext = new JavaSparkContext(sparkConfiguration);

			System.out
					.println("SparkContext initialized, connected to master: "
							+ args[0]);

			while (true) {

				System.out.print("> ");

				String[] arguments = new BufferedReader(new InputStreamReader(
						System.in)).readLine().trim().split("\\s+");

				if (!arguments[0].equals("exit"))
					new RefineOnSpark().doMain(arguments);
				else
					return;
			}

		} catch (CmdLineException e) {
			printUsage(parser);
			System.exit(-1);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (sparkContext != null)
				sparkContext.stop();
		}
	}

	private void doMain(String jobArguments[]) {
		JobOptions options = new JobOptions();
		CmdLineParser parser = new CmdLineParser(options);

		try {
			parser.parseArgument(jobArguments);

			if (options.fArguments.size() < 2) {
				throw new CmdLineException(parser,
						"Two arguments are required!");
			}

			File inputFile = checkExists(options.fArguments.get(0));
			File transformFile = checkExists(options.fArguments.get(1));

			sparkContext.addFile(transformFile.getAbsoluteFile().toString());

			JavaRDD<String> lines;

			if (options.numPartitions == null)
				lines = sparkContext.textFile(inputFile.getAbsoluteFile()
						.toString());
			else
				lines = sparkContext.textFile(inputFile.getAbsoluteFile()
						.toString(), options.numPartitions);

			Broadcast<String> header = sparkContext.broadcast(lines.first());

			lines = lines.mapPartitions(new JobFunction(header, transformFile
					.getName()));

			
			lines.saveAsTextFile(inputFile.getName());
			

		} catch (CmdLineException e) {
			printUsageJob(parser);
		} catch (Exception e) {
			System.err.println("Caught Exception, cause: " + e.getMessage());
			printUsage(parser);
		}
	}

	private static void printUsage(CmdLineParser parser) {
		System.err.println("Usage: refineonspark [OPTION...] SPARK://MASTER:PORT\n");
		parser.printUsage(System.err);
	}

	private static void printUsageJob(CmdLineParser parser) {
		System.err.println("Usage: [OPTION...] INPUTFILE TRANSFORM\n");
		parser.printUsage(System.err);
	}

	private File checkExists(String name) throws FileNotFoundException {
		File file = new File(name);
		if (!file.exists()) {
			throw new FileNotFoundException("File " + name
					+ " could not be found.");
		}
		return file;
	}

}
