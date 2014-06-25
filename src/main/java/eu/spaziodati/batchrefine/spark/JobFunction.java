package eu.spaziodati.batchrefine.spark;

import java.io.File;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.json.JSONArray;

import eu.spaziodati.batchrefine.spark.http.SparkRefineHTTPClient;
import eu.spaziodati.batchrefine.spark.utils.RDDContentBody;

public class JobFunction implements FlatMapFunction<Iterator<String>, String>,
		Serializable {
	private static final long serialVersionUID = 1401955470896751634L;
	private Broadcast<String> header;
	private String transformFile;

	public JobFunction(Broadcast<String> header, String transformFile) {
		this.header = header;
		this.transformFile = transformFile;
	};

	@Override
	public List<String> call(Iterator<String> originalRDD) throws Exception {
		List<String> transformed = null;

		Properties exporterProperties = new Properties();
		exporterProperties.setProperty("format", "csv");

		JSONArray transformArray = new JSONArray(
				FileUtils.readFileToString(new File(SparkFiles
						.get(transformFile))));

		SparkRefineHTTPClient client = new SparkRefineHTTPClient("localhost",
				3333);

		transformed = client.transform(
				new RDDContentBody(originalRDD, header.getValue()),
				transformArray, exporterProperties);

		transformed.remove(0);

		return transformed;

	}

}