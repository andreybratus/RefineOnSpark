package eu.spaziodati.batchrefine.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import junit.framework.AssertionFailedError;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.Assert;

/**
 * Helper methods for RefineOnSpark test classes.
 * @author andrey
 *
 */
public class TestUtils {

	/**
	 * Filter to list only the "part-***" files in the folder
	 * 
	 * @author andrey
	 */
	static class PartFileFilter implements PathFilter {
		@Override
		public boolean accept(Path path) {
			if (path.getName().contains("part"))
				return true;
			else
				return false;
		}
	}

	/**
	 * Take all the 'part-***' files from the source directory and write them
	 * into a single dstFile. if dstFile exists, will be overwritten.
	 * 
	 * @param srcDir
	 * @param dstFile
	 * @throws IOException
	 */
	public static void copyMergeLocal(Path srcDir, Path dstFile)
			throws IOException {
		Configuration conf = new Configuration(true);
		conf.setBoolean("hadoop.native.lib", false);

		FileSystem srcFs = srcDir.getFileSystem(conf);

		FSDataOutputStream out = FileSystem.getLocal(conf).getRawFileSystem()
				.create(dstFile, true);

		try {

			FileStatus contents[] = srcFs.globStatus(srcDir.suffix("/*"),
					new PartFileFilter());

			for (int i = 0; i < contents.length; i++) {
				FSDataInputStream in = srcFs.open(contents[i].getPath());
				try {
					IOUtils.copyBytes(in, out, conf, false);
				} finally {
					in.close();
				}
			}
		} finally {
			out.close();
		}
		srcFs.delete(srcDir, true);
	}

	/**
	 * Matches two files line by line without loading any of them into memory.
	 * 
	 * @throws AssertionFailedError
	 *             if the files do not match.
	 */
	public static void assertContentEquals(File expectedFile, File outputFile)
			throws IOException {

		BufferedReader expected = null;
		BufferedReader output = null;

		try {
			expected = new BufferedReader(new FileReader(expectedFile));
			output = new BufferedReader(new FileReader(outputFile));

			int line = 0;
			String current = null;

			do {
				current = expected.readLine();
				String actual = output.readLine();

				if (current == null) {
					if (actual != null) {
						Assert.fail("Actual output too short (line " + line
								+ ").");
					}
					break;
				}

				if (!current.equals(actual)) {
					Assert.fail("Expected: " + current + "\n Got: " + actual
							+ "\n at line " + line);
				}

			} while (current != null);

		} finally {
			org.apache.commons.io.IOUtils.closeQuietly(expected);
			org.apache.commons.io.IOUtils.closeQuietly(output);
			outputFile.delete();
		}
	}
}
