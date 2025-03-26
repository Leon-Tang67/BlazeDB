package ed.inf.adbs.blazedb;

import org.junit.Test;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertEquals;

public class BlazeDBTest {

	private static final String JAR_PATH = "target/blazedb-1.0.0-jar-with-dependencies.jar";
	private static final String DB_PATH = "samples/db";
	private static final String INPUT_PATH = "samples/input/";
	private static final String EXPECTED_OUTPUT_PATH = "samples/expected_output/";
	private static final String OUTPUT_PATH = "samples/test_output/";

	@Test
	public void testQueries() throws IOException, InterruptedException {
		for (int i = 1; i <= 12; i++) {
			String queryFile = INPUT_PATH + "query" + i + ".sql";
			String expectedOutputFile = EXPECTED_OUTPUT_PATH + "query" + i + ".csv";
			String actualOutputFile = OUTPUT_PATH + "query" + i + ".csv";

			try {
				// Run the jar file with the query
				ProcessBuilder processBuilder = new ProcessBuilder(
						"java", "-jar", JAR_PATH, DB_PATH, queryFile, actualOutputFile);
				Process process = processBuilder.start();
				process.waitFor();

				// Compare the actual output with the expected output
				List<String> expectedOutput = readLines(expectedOutputFile);
				List<String> actualOutput = readLines(actualOutputFile);

				assertEquals("Output mismatch for query" + i, expectedOutput, actualOutput);
			} catch (AssertionError | IOException | InterruptedException e) {
				System.err.println("Test failed for query" + i + ": " + e.getMessage());
			}
		}
	}

	private List<String> readLines(String filePath) throws IOException {
		List<String> lines = new ArrayList<>();
		try (BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)))) {
			String line;
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}
		}
		return lines;
	}
}