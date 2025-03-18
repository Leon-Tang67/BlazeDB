package ed.inf.adbs.blazedb;

import java.io.*;

import ed.inf.adbs.blazedb.operator.ScanOperator;
//import ed.inf.adbs.blazedb.operator.SelectOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import ed.inf.adbs.blazedb.operator.Operator;
//import ed.inf.adbs.blazedb.ExpressionEvaluator;

/**
 * Lightweight in-memory database system.
 *
 * Feel free to modify/move the provided functions. However, you must keep
 * the existing command-line interface, which consists of three arguments.
 *
 */
public class BlazeDB {

	public static void main(String[] args) {

//		if (args.length != 3) {
//			System.err.println("Usage: BlazeDB database_dir input_file output_file");
//			return;
//		}
//
//		String databaseDir = args[0];
//		String inputFile = args[1];
//		String outputFile = args[2];

//		String databaseDir = "samples/db";
//		String inputFile = "samples/input/query4.sql";
//		String outputFile = "samples/output/query4.csv";
//
//		try {
//			// Initialize DatabaseCatalog
//			DatabaseCatalog.getInstance(databaseDir);
//
//			// Parse query using JSQLParser
//			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
//			if (statement != null) {
//				PlainSelect select = (PlainSelect) statement;
//				String tableName = select.getFromItem().toString();
//				Expression expression = select.getWhere();
//
//				// Execute query using ScanOperator
//				ScanOperator scanOperator = new ScanOperator(tableName);
//				SelectOperator selectOperator = new SelectOperator(scanOperator, expression);
//				execute(selectOperator, outputFile);
//			} else {
//				System.out.println("Unsupported query type.");
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}

		parsingExample("inputFile");
	}


	/**
	 * Executes the provided query plan by repeatedly calling `getNextTuple()`
	 * on the root object of the operator tree. Writes the result to `outputFile`.
	 *
	 * @param root The root operator of the operator tree (assumed to be non-null).
	 * @param outputFile The name of the file where the result will be written.
	 */
	public static void execute(Operator root, String outputFile) {
		try {
			// Create a BufferedWriter
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

			// Iterate over the tuples produced by root
			Tuple tuple = root.getNextTuple();
			while (tuple != null) {
				writer.write(tuple.toString());
				writer.newLine();
				tuple = root.getNextTuple();
			}

			// Close the writer
			writer.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}


	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement
	 * from a file or a string and prints the SELECT and WHERE clauses to screen.
	 */

	public static void parsingExample(String filename) {
		try {
//			Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
//          Statement statement = CCJSqlParserUtil.parse("SELECT Course.cid, Student.name FROM Course, Student WHERE Student.sid = 3");
			int query_selector = 5;
			Statement statement = null;
			switch (query_selector) {
				case 1:
					statement = CCJSqlParserUtil.parse("SELECT * FROM Student;");
					break;
				case 2:
					statement = CCJSqlParserUtil.parse("SELECT Student.A FROM Student;");
					break;
				case 3:
					statement = CCJSqlParserUtil.parse("SELECT Student.D, Student.B, Student.A FROM Student;");
					break;
				case 4:
					statement = CCJSqlParserUtil.parse("SELECT * FROM Student WHERE Student.A < 3;");
					break;
				case 5:
					statement = CCJSqlParserUtil.parse("SELECT * FROM Student, Enrolled, Course WHERE Student.A = Enrolled.A AND Enrolled.E = Course.E;");
					break;
				case 6:
					statement = CCJSqlParserUtil.parse("SELECT * FROM Student, Course WHERE Student.C < Course.E;");
					break;
				case 7:
					statement = CCJSqlParserUtil.parse("SELECT DISTINCT Enrolled.A FROM Enrolled;");
					break;
				case 8:
					statement = CCJSqlParserUtil.parse("SELECT * FROM Student ORDER BY Student.B;");
					break;
				case 9:
					statement = CCJSqlParserUtil.parse("SELECT Enrolled.E, SUM(Enrolled.H * Enrolled.H) FROM Enrolled GROUP BY Enrolled.E;");
					break;
				case 10:
					statement = CCJSqlParserUtil.parse("SELECT SUM(1) FROM Student GROUP BY Student.B;");
					break;
				case 11:
					statement = CCJSqlParserUtil.parse("SELECT Student.B, Student.C FROM Student, Enrolled WHERE Student.A = Enrolled.A GROUP BY Student.B, Student.C ORDER BY Student.C, Student.B;");
					break;
				case 12:
					statement = CCJSqlParserUtil.parse("SELECT SUM(1), SUM(Student.A) FROM Student, Enrolled;");
					break;
			}
			if (statement != null) {
				PlainSelect select = (PlainSelect) statement;
				System.out.println("Statement: " + select);
				System.out.println("SELECT items: " + select.getSelectItems());
				System.out.println("WHERE expression: " + select.getWhere().getClass().getName());
				System.out.println("From item: " + select.getFromItem().getClass().getName());
				System.out.println("Joins: " + select.getJoins().get(0).getClass().getName());
				System.out.println("Group by: " + select.getGroupBy());
				System.out.println("Order by: " + select.getOrderByElements());
				System.out.println("Distinct: " + select.getDistinct());
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
}
