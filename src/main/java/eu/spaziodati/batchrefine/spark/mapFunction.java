package eu.spaziodati.batchrefine.spark;

import java.io.Serializable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class mapFunction implements
		Function<Tuple2<LongWritable, Text>, String>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5123651826193857150L;

	@Override
	public String call(Tuple2<LongWritable, Text> v1) throws Exception {
		return v1._2.toString();
	}
}