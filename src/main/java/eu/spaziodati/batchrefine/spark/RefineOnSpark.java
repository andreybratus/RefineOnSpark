package eu.spaziodati.batchrefine.spark;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import eu.spaziodati.batchrefine.spark.http.SparkRefineHTTPClient;
import eu.spaziodati.batchrefine.spark.utils.DriverOptions;
import eu.spaziodati.batchrefine.spark.utils.JobOptions;

public class RefineOnSpark implements RemoteInterface {

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

	public RefineOnSpark() {
	};

	public static void main(String[] args) {
		ServerSocket stubServer = null;
		ObjectOutputStream objStream = null;
		DriverOptions options = new DriverOptions();

		CmdLineParser parser = new CmdLineParser(options);
		try {
			parser.parseArgument(args);

			if (options.getArguments().size() < 1) {
				printUsage(parser);
				System.exit(-1);
			}

			String currentWorkDir = System.getProperty("user.dir");
			System.out.println(currentWorkDir);
			
			SparkConf sparkConfiguration = new SparkConf(true)
					.setMaster(options.getArguments().get(0))
					.setAppName(options.getAppName())
					.set("spark.executor.memory", options.getExecutorMemory())
					.set("spark.executor.extraClassPath",
							currentWorkDir + "/RefineOnSpark-0.1.jar:"
									+ currentWorkDir + "/lib/*");

			System.out.println(sparkConfiguration.get("spark.executor.extraClassPath"));
			sparkContext = new JavaSparkContext(sparkConfiguration);

			System.out
					.println("SparkContext initialized, connected to master: "
							+ args[0]);

			while (true) {
				try {
					stubServer = new ServerSocket(3377);

					RefineOnSpark obj = new RefineOnSpark();

					RemoteInterface stub = (RemoteInterface) UnicastRemoteObject
							.exportObject(obj, 0);

					Socket connection = stubServer.accept();
					objStream = new ObjectOutputStream(
							connection.getOutputStream());

					objStream.writeObject(stub);
					objStream.close();

					System.err.println("Connection accepted! From: "
							+ connection.getRemoteSocketAddress().toString());
				} catch (Exception e) {
					System.err.println("Failed accepting request: "
							+ e.toString());
					e.printStackTrace();
				} finally {
					IOUtils.closeQuietly(stubServer);
				}
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

	public double doMain(String[] jobArguments) throws Exception {
		JobOptions options = new JobOptions();
		CmdLineParser parser = new CmdLineParser(options);
		long startTime = System.nanoTime();

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

			List<String> result = lines.collect();

			FileUtils.writeLines(
					new File(FilenameUtils.removeExtension(inputFile.getName())
							+ "_out.csv"), result);

		} catch (CmdLineException e) {
			printUsageJob(parser);
			throw e;
		} catch (Exception e) {
			System.err.println("Caught Exception, cause: " + e.getMessage());
			printUsage(parser);
			throw e;
		} finally {
			sparkContext.cancelAllJobs();
		}

		return (System.nanoTime() - startTime) / 1000000000.0;
	}

	private static void printUsage(CmdLineParser parser) {
		System.err
				.println("Usage: refineonspark [OPTION...] SPARK://MASTER:PORT\n");
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
