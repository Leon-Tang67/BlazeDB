package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Interpreter;
import ed.inf.adbs.blazedb.QueryPlanner;
import ed.inf.adbs.blazedb.Tuple;
import junit.framework.TestCase;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class DuplicateEliminationOperatorTest extends TestCase {

    public void testDistinctOperator() {
        singleTestDistinctOperator();
//        multiTestDistinctOperator();
    }

    public void multiTestDistinctOperator() {
        String databaseDir = "samples/db";
        String inputFile = "samples/input/distinct-queries.sql";
        String outputFile = "samples/output/distinct-queries.csv";

        try {
            // Initialize DatabaseCatalog
            DatabaseCatalog.getInstance(databaseDir);

            // Read queries from input file
            List<String> queries = Files.readAllLines(Paths.get(inputFile));

            // Execute each query and append results to output file
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile))) {
                int i = 1;
                for (String query : queries) {
                    Statement statement = CCJSqlParserUtil.parse(query);

                    Select selectStatement = (Select) statement;
                    PlainSelect plainSelect = (PlainSelect) selectStatement;

                    // Generate query plan
                    QueryPlanner planner = new QueryPlanner(plainSelect);
                    Operator rootOperator = planner.generatePlan();

                    // Write result to output file with separation
                    writer.write("Query " + i++ + ":\n");
                    Tuple tuple;
                    while ((tuple = rootOperator.getNextTuple()) != null) {
                        writer.write(tuple.toString());
                        writer.newLine();
                    }
                    writer.write("\n---\n");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void singleTestDistinctOperator() {
        String databaseDir = "samples/db";
        String inputFile = "samples/input/query.sql";
        String outputFile = "samples/output/query.csv";

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