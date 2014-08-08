package eu.spaziodati.batchrefine.spark;

import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import eu.spaziodati.batchrefine.spark.utils.DriverCLIOptions;
import eu.spaziodati.batchrefine.spark.utils.StringAccumulatorParam;

/**
 * This is a simple Spark Driver CLI program, aimed to connect to the Spark
 * cluster, with specified options in the {@link DriverOptions}. After
 * connecting to the master, block and waits for a connection
 * 
 * @author andrey
 */

public class RefineOnSpark implements RemoteInterface {

	private static JavaSparkContext sparkContext;
	public static RemoteInterface stub;
	private static RefineOnSpark obj;

	public RefineOnSpark() {
	};

	/**
	 * Server main function:<br>
	 * - initializes the {@link JavaSparkContext}<br>
	 * - connect to the master node specified in args[0],<br>
	 * - listens for socket connection from a client at port {@code 3377}
	 * 
	 * @param args
	 */

	public static void main(String[] args) {
		ServerSocket stubServer = null;
		ObjectOutputStream objStream = null;
		DriverCLIOptions cLineOptions = new DriverCLIOptions();

		CmdLineParser parser = new CmdLineParser(cLineOptions);

		try {
			parser.parseArgument(args);

			if (cLineOptions.getArguments().size() < 1) {
				printUsage(parser);
				System.exit(-1);
			}

			sparkContext = new JavaSparkContext(configureSpark(cLineOptions));

			System.out
					.println("SparkContext initialized, connected to master: "
							+ args[0]);
			System.out.println("Waiting for connections");

			obj = new RefineOnSpark();
			stub = (RemoteInterface) UnicastRemoteObject.exportObject(obj, 0);

			while (true) {
				try {
					stubServer = new ServerSocket(3377);

					Socket connection = stubServer.accept();

					System.err.println("Connection accepted! From: "
							+ connection.getRemoteSocketAddress().toString()
							+ " Sending stub!");

					objStream = new ObjectOutputStream(
							connection.getOutputStream());

					objStream.writeObject(stub);
					objStream.close();

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

	/**
	 * Function called using RMI through {@link RemoteInterface} which submits a
	 * job to {@code sparkContext} and returns the processing time
	 * 
	 * @return processing time
	 */

	public String submitJob(String[] options) throws Exception {

		long startTime = System.nanoTime();
		long transFormTime;
		Accumulator<String> accum;
		try {

			JavaRDD<String> lines;

			// if option number of partitions is not set, Spark will
			// automatically partition the file by trying to fill the block
			// size
			if (options[3].equals("0"))
				lines = sparkContext.textFile(options[0]);
			else
				lines = sparkContext.textFile(options[0],
						Integer.parseInt(options[3]));

			// lines = sparkContext.hadoopFile(options[0],
			// TextInputFormat.class,
			// LongWritable.class, Text.class,
			// Integer.parseInt(options[3])).map(new mapFunction());

			// broadcast header so that all worker nodes can read it.
			Broadcast<String> header = sparkContext.broadcast(lines.first());

			// each worker appends processing time to this accumulator<String>
			accum = sparkContext.accumulator(new String(),
					new StringAccumulatorParam());

			// assign job to each partition.
			lines = lines.mapPartitions(new JobFunction(header, options[1],
					accum));

			lines.saveAsTextFile(options[2]);

			transFormTime = System.nanoTime() - startTime;

		} catch (Exception e) {
			System.err.println("Caught Exception, cause: " + e.getMessage());
			throw new RemoteException(e.getMessage());
		} finally {
			sparkContext.cancelAllJobs();
		}

		return String.format("%18s\t%2.3f%s",
				FilenameUtils.getName(options[0]),
				transFormTime / 1000000000.0, accum.value());
	}

	private static SparkConf configureSpark(DriverCLIOptions cLineOptions) {

		String currentWorkDir = System.getProperty("user.dir");

		SparkConf sparkConfiguration = new SparkConf(true)
				.setMaster(cLineOptions.getArguments().get(0))
				.setAppName(cLineOptions.getAppName())
				.set("spark.executor.memory", cLineOptions.getExecutorMemory())
				.set("spark.executor.extraClassPath",
						currentWorkDir + "/RefineOnSpark-0.1.jar:"
								+ currentWorkDir + "/lib/*")
				.set("spark.hadoop.fs.local.block.size",
						cLineOptions.getFsBlockSize().toString());
		return sparkConfiguration;
	}

	/**
	 * Print usage for the main() spark driver app initialization function
	 * 
	 * @param parser
	 */
	private static void printUsage(CmdLineParser parser) {
		System.err
				.println("Usage: refineonspark [OPTION...] SPARK://MASTER:PORT\n");
		parser.printUsage(System.err);
	}

}
