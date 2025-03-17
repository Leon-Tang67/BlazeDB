package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Interpreter;
import junit.framework.TestCase;

public class ProjectOperatorTest extends TestCase {

    public void testProjectOperator() {
        String databaseDir = "samples/db";
        String inputFile = "samples/input/query3.sql";
        String outputFile = "samples/output/query3.csv";

        try {
            // Initialize DatabaseCatalog
            DatabaseCatalog.getInstance(databaseDir);

            // Execute query from input file
            Interpreter.executeQuery(inputFile, outputFile);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}