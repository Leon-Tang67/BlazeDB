package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Interpreter;
import junit.framework.TestCase;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.FileReader;

import ed.inf.adbs.blazedb.Interpreter;

public class ScanOperatorTest extends TestCase {

    public void testScanOperator() {
        String databaseDir = "samples/db";
		String inputFile = "samples/input/query1.sql";
		String outputFile = "samples/output/query1.csv";

        try {
            // Initialize DatabaseCatalog
            DatabaseCatalog.getInstance(databaseDir);

            // Parse query using JSQLParser
            Interpreter.executeQuery(inputFile, outputFile);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}