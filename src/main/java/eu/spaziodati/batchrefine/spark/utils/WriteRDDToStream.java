package eu.spaziodati.batchrefine.spark.utils;

import java.io.PipedOutputStream;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;

public class WriteRDDToStream implements Runnable {

	private Iterator<String> rdd;
	private PipedOutputStream out;
	private String header;

	public WriteRDDToStream(Iterator<String> rdd, PipedOutputStream out, String header) {
		this.rdd = rdd;
		this.out = out;
		this.header = header;
	}

	@Override
	public void run() {
		try {

			if (rdd.hasNext()) {
				String firstLine = rdd.next();
				if (firstLine.equals(header)) {
					out.write(header.getBytes());
					out.write(("\n").getBytes());
				} else {
					out.write(header.getBytes());
					out.write(("\n").getBytes());
					out.write(firstLine.getBytes());
					out.write(("\n").getBytes());
				}

				while (rdd.hasNext()) {
					out.write(rdd.next().getBytes());
					out.write(("\n").getBytes());
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.closeQuietly(out);
		}
	}

}
