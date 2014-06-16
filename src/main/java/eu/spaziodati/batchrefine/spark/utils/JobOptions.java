package eu.spaziodati.batchrefine.spark.utils;

import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

public class JobOptions {

@Option (name = "-o", usage = "specify output filename. default: no output",required = false)
public String outputFilename;

@Option (name = "-p", aliases ={"--partitions"}, usage = "specify number of partitions to split INPUTFILE. default: auto",required= false)
public Integer numPartitions;


@Argument
public List<String> fArguments= new ArrayList<String>();



public String getOutputFileName() {
	if (outputFilename == null)
		return "output_" + fArguments.get(0);
	else
		return outputFilename;
}

}
