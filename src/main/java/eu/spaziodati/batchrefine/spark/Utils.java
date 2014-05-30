package eu.spaziodati.batchrefine.spark;
import java.io.Closeable;
import java.io.IOException;

public class Utils {

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
