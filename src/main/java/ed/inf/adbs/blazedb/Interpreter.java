package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.Operator;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import java.io.*;

public class Interpreter {
    public static void executeQuery(String queryFile, String outputFile) {
        try (BufferedReader reader = new BufferedReader(new FileReader(queryFile))) {
            String query = reader.readLine(); // Assume one query per file
            Statement statement = CCJSqlParserUtil.parse(query);

            // Generate query plan
            QueryPlanner planner = new QueryPlanner();
            Operator rootOperator = planner.createQueryPlan(statement);

            // Execute and write output
            execute(rootOperator, outputFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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
