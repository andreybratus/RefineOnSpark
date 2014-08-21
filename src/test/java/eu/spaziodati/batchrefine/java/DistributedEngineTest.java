package eu.spaziodati.batchrefine.java;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.spaziodati.batchrefine.java.TestUtils;
import eu.spaziodati.batchrefine.spark.RefineOnSpark;

@RunWith(Parameterized.class)
public class DistributedEngineTest {

	protected final String fInput;
	protected final String fTransform;
	protected final String fOutputFolder;
	protected final String fExpectedFile;
	private static RefineOnSpark driverInstance;

	public DistributedEngineTest(String input, String transform) {
		this.fInput = DistributedEngineTest.class.getClassLoader()
				.getResource("input/" + input).getPath();
		this.fTransform = DistributedEngineTest.class.getClassLoader()
				.getResource("operations/" + transform + ".json").getPath();
		this.fOutputFolder = "/tmp/" + transform;
		this.fExpectedFile = DistributedEngineTest.class.getClassLoader()
				.getResource("expected/" + transform + ".csv").getPath();
	}

	@Parameters(name = "{0} : {1}")
	public static Collection<String[]> inputData() {
		return Arrays
				.asList(new String[][] { { "50.csv", "column-addition" },
						{ "50.csv", "column-removal" },
						{ "50.csv", "blank_down" },
						{ "50.csv", "column-split" },
						{ "50.csv", "compositetransform" },
						{ "50.csv", "blankout-cells" },
						{ "50.csv", "key-value-columnize" },
						{ "50.csv", "column-move" }, { "50.csv", "mass-edit" },
						{ "50.csv", "text-transform" },
						{ "50.csv", "reorder_rows" },
						{ "50.csv", "transp-col-rows" },
						{ "50.csv", "transp-row-col" } });
	}

	@Before
	public void removeTempFiles() throws IOException {
		FileUtils.deleteDirectory(new File(fOutputFolder));
	}

	@Test
	public void test() throws Exception {
		driverInstance.submitJob(new String[] { fInput, fTransform,
				fOutputFolder, "5" });

		TestUtils.copyMergeLocal(new Path(fOutputFolder), new Path(
				"/tmp/tmp.out"));

		TestUtils.assertContentEquals(new File(fExpectedFile), new File(
				"/tmp/tmp.out"));
	}

	@BeforeClass
	public static void setUp() {
		System.out.println("Initializing SparkContext");
		driverInstance = new RefineOnSpark();
	}

	@AfterClass
	public static void tearDown() {
		System.out.println("Shutting down SparkContext");
		driverInstance.close();
	}

}
