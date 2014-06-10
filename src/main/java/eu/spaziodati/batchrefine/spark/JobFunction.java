package eu.spaziodati.batchrefine.spark;


import java.io.File;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.json.JSONArray;

import eu.spaziodati.batchrefine.spark.http.SparkRefineHTTPClient;

public class JobFunction implements FlatMapFunction<Iterator<String>, String>,
		Serializable {
	private static final long serialVersionUID = 1401955470896751634L;
	private Broadcast<String> header;
	private String operations;

	public JobFunction(Broadcast<String> header, String operations) {
		this.header = header;
		this.operations = operations;
	};

	@Override
	public Iterable<String> call(Iterator<String> original) throws Exception {
		List<String> transformed = null;

		Properties exporterProperties = new Properties();
		exporterProperties.setProperty("format", "csv");

		JSONArray transformArray = new JSONArray(
				FileUtils.readFileToString(new File(operations)));

		SparkRefineHTTPClient client = new SparkRefineHTTPClient("localhost",
				3333);		
		
		transformed = client.transform(original, transformArray,
				exporterProperties, header.getValue());

		
		transformed.remove(0);
	
		return transformed;
		
	}
}