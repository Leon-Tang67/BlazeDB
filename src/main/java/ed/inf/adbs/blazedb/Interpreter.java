package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.Operator;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.*;

/**
 * The Interpreter class is responsible for executing the query in the provided file
 * and writing the result to the output file.
 *
 * The Interpreter class contains the following methods:
 * - executeQuery(): Executes the query in the provided file and writes the result to the output file.
 * - execute(): Executes the provided query plan by repeatedly calling `getNextTuple()`
 */

 public class Interpreter {
    /**
     * Executes the query in the provided file and call the execute method to write the result to the output file.
     *
     * @param queryFile The name of the file containing the query.
     * @param outputFile The name of the file where the result will be written.
     */
    public static void executeQuery(String queryFile, String outputFile) {
        try (BufferedReader reader = new BufferedReader(new FileReader(queryFile))) {
            String query = reader.readLine(); // Assume one query per file
            Statement statement = CCJSqlParserUtil.parse(query);

            Select selectStatement = (Select) statement;
            PlainSelect plainSelect = (PlainSelect) selectStatement;

            // Generate query plan
            QueryPlanner planner = new QueryPlanner(plainSelect);
            Operator rootOperator = planner.generatePlan();

            // Execute and write output
            execute(rootOperator, outputFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Executes the provided query plan by repeatedly calling `getNextTuple()`
     * on the root object of the operator tree. Writes the result to `outputFile`.
     *
     * @param root The root operator of the operator tree (assumed to be non-null).
     * @param outputFile The name of the file where the result will be written.
     */
    private static void execute(Operator root, String outputFile) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            Tuple tuple;
            while ((tuple = root.getNextTuple()) != null) {
                writer.write(tuple.toString());
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
