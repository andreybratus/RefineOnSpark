package eu.spaziodati.batchrefine.spark;

import java.io.File;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.json.JSONArray;

import eu.spaziodati.batchrefine.spark.http.SparkRefineHTTPClient;
import eu.spaziodati.batchrefine.spark.utils.RDDContentBody;

/**
 * Function that is run on the worker node:<br>
 * 
 * @param header
 *            is the Spark broadcast variable, containing the original header
 * @param transformfile
 *            path to the JSON transform operations
 * @param accum
 *            Spark {@link Accumulator}{@code<String>} used to append the worker
 *            processing time to it that will be read on the master.
 * @author andrey
 */

public class JobFunction implements FlatMapFunction<Iterator<String>, String>,
		Serializable {
	private static final long serialVersionUID = 1401955470896751634L;
	private Broadcast<String> header;
	private String transformFile;
	private Accumulator<String> accum;

	public JobFunction(Broadcast<String> header, String transformFile,
			Accumulator<String> accum) {
		this.header = header;
		this.transformFile = transformFile;
		this.accum = accum;
	};

	/**
	 * Processes the passed chunk of data by submitting it to Refine, using
	 * {@link SparkRefineHTTPClient} and returning the transformed data.<br>
	 * Deals with the presence/absence of the header in obtained chunk:<br>
	 * - if a partition has a header, leave it. - if partition doesn't have a
	 * header, add it, and than remove it on transformed data.
	 * 
	 * @param originalRDD
	 *            {@code Iterator} over the chunk RDD
	 * @return transformed {@code List<String>} output from Refine
	 */

	@Override
	public List<String> call(Iterator<String> originalRDD) throws Exception {
		List<String> transformed = null;

		long startTime = System.nanoTime();

		Properties exporterProperties = new Properties();
		exporterProperties.setProperty("format", "csv");

		JSONArray transformArray = new JSONArray(
				FileUtils.readFileToString(new File(transformFile)));

		SparkRefineHTTPClient client = new SparkRefineHTTPClient("localhost",
				3333);

		RDDContentBody RDDchunk = new RDDContentBody(originalRDD,
				header.getValue());

		transformed = client.transform(RDDchunk, transformArray,
				exporterProperties);

		if (!RDDchunk.hadHeader())
			transformed.remove(0);

		accum.add(String.format("\t%2.3f",
				(System.nanoTime() - startTime) / 1000000000.0));

		return transformed;

	}

}