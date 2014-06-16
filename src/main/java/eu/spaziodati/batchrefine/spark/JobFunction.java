package eu.spaziodati.batchrefine.spark;

import java.io.File;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.http.entity.ContentType;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.json.JSONArray;

import eu.spaziodati.batchrefine.spark.http.SparkRefineHTTPClient;
import eu.spaziodati.batchrefine.spark.utils.FakeFileBody;

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
	public List<String> call(Iterator<String> original) throws Exception {
		List<String> transformed = null;

		Properties exporterProperties = new Properties();
		exporterProperties.setProperty("format", "csv");

		JSONArray transformArray = new JSONArray(
				FileUtils.readFileToString(new File(SparkFiles
						.get(transformFile))));

		SparkRefineHTTPClient client = new SparkRefineHTTPClient("localhost",
				3333);

		StringBuilder sb = new StringBuilder();

		if (original.hasNext()) {
			String firstLine = original.next();

			if (firstLine.equals(header.getValue())) {
				sb.append(firstLine);
				sb.append("\n");
			} else {
				sb.append(header.getValue());
				sb.append("\n");
				sb.append(firstLine);
				sb.append("\n");
			}
		}

		while (original.hasNext()) {
			sb.append(original.next());
			sb.append("\n");
		}

		transformed = client.transform(new FakeFileBody(sb.toString(),
				ContentType.MULTIPART_FORM_DATA, header.getValue()),
				transformArray, exporterProperties);

		return transformed;

	}

}