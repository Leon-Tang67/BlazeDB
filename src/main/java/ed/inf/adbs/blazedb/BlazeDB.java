package ed.inf.adbs.blazedb;

/**
 * BlazeDB is a lightweight in-memory database system that supports SQL queries with SELECT statements.
 * It reads the schema from a database directory and executes queries from input files.
 * The results are written to output files.
 * The database directory contains CSV files with the data for each table.
 * The input file contains one query per line.
 * The output file contains the result of each query.
 * The database system supports the following SQL operations:
 * - SELECT, FROM, WHERE, ORDER BY, DISTINCT, GROUP BY, SUM
 */
public class BlazeDB {

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: BlazeDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		try {
			// Initialize DatabaseCatalog, read schema from database directory and save to memory
			DatabaseCatalog.getInstance(databaseDir);

			// Execute query from input file
			Interpreter.executeQuery(inputFile, outputFile);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}