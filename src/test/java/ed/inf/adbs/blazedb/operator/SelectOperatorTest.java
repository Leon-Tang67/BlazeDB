package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Interpreter;
import junit.framework.TestCase;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.FileReader;

public class SelectOperatorTest extends TestCase {

    public void testSelectOperator() {
        String databaseDir = "samples/db";
        String inputFile = "samples/input/query4.sql";
        String outputFile = "samples/output/query4.csv";

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