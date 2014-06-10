package eu.spaziodati.batchrefine.spark.utils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.content.FileBody;

/**
 * As RefineHTTPClient relies on multipart-form-data to upload a file
 * we need this small hack to  convert a String into binary {@link FileBody}
 * in order to use it with {@link RefineHTTPClient}
 * 
 * @see {@link FileBody}
 * 
 * @author andrey
 *
 */

public class FakeFileBody extends FileBody {
	private byte[] bytes;

	public FakeFileBody(final String input, final ContentType contentType,
			final String fileName) {
		super(new File("."), contentType, fileName);
		bytes = input.getBytes();
	}

	@Override
	public void writeTo(final OutputStream out) throws IOException {
		out.write(bytes);
	}

	@Override
	public long getContentLength() {
		return bytes.length;
	}

}