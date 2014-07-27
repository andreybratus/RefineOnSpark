package eu.spaziodati.batchrefine.spark.utils;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;


public class Utils {

	public static void writeIteratorToFile(File file, Iterator<String> iter) throws IOException{
		if (!iter.hasNext()){
			return;
		}
		FileOutputStream out = null;
		String lineEnding = IOUtils.LINE_SEPARATOR;
		try{
			out = FileUtils.openOutputStream(file);
			final BufferedOutputStream buffer = new BufferedOutputStream(out);
			while(iter.hasNext()){
				buffer.write(iter.next().getBytes());
				buffer.write(lineEnding.getBytes());
			}
			buffer.flush();
			out.close();
		} finally {
			IOUtils.closeQuietly(out);
		}
	}

	/**
	 * Closes a given {@link Closeable} if it's not <code>null</code>.
	 * 
	 * @param closeable
	 *            the {@link Closeable} to be closed.
	 * @param rethrow
	 *            if set to <code>true</code>, rethrows any exceptions
	 *            encountered during {@link Closeable#close()}.
	 * 
	 * @throws IOException
	 *             if an exception is thrown, and <code>rethrow</code> is set to
	 *             true.
	 */
	public static void safeClose(Closeable closeable, boolean rethrow)
			throws IOException {

		if (closeable != null) {
			try {
				closeable.close();
			} catch (IOException ex) {
				if (rethrow) {
					throw ex;
				} else {
					ex.printStackTrace();
				}
			}
		}
	}

}
