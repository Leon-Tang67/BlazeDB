package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * Lightweight in-memory database system.
 *
 * Feel free to modify/move the provided functions. However, you must keep
 * the existing command-line interface, which consists of three arguments.
 *
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
			// Initialize DatabaseCatalog
			DatabaseCatalog.getInstance(databaseDir);

			// Execute query from input file
			Interpreter.executeQuery(inputFile, outputFile);

		} catch (Exception e) {
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
			int query_selector = 9;
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
					statement = CCJSqlParserUtil.parse("SELECT DISTINCT Student.A, Enrolled.A FROM Student, Enrolled WHERE Student.A = Enrolled.A;");
					break;
				case 8:
					statement = CCJSqlParserUtil.parse("SELECT * FROM Student ORDER BY Student.B;");
					break;
				case 9:
					statement = CCJSqlParserUtil.parse("SELECT Enrolled.E, SUM(2 * Enrolled.H * Enrolled.E) FROM Enrolled GROUP BY Enrolled.E, Enrolled.H;");
					break;
				case 10:
					statement = CCJSqlParserUtil.parse("SELECT SUM(1) FROM Student GROUP BY Student.B;");
					break;
				case 11:
					statement = CCJSqlParserUtil.parse("SELECT Student.B, Student.C FROM Student, Enrolled WHERE Student.A = Enrolled.A GROUP BY Student.B, Student.C ORDER BY Student.C, Student.B;");
					break;
				case 12:
					statement = CCJSqlParserUtil.parse("SELECT SUM(1), SUM(Student.A) FROM Student, Enrolled GROUP BY Student.B, Student.C;");
					break;
			}
			if (statement != null) {
				PlainSelect select = (PlainSelect) statement;
				System.out.println("Statement: " + select);
				System.out.println("SELECT 1st item: " + select.getSelectItems().get(0).getExpression().getClass().getName());
				System.out.println("SELECT 2nd item: " + ((Function) select.getSelectItems().get(1).getExpression()).getParameters());
				System.out.println("WHERE expression: " + select.getWhere());
				System.out.println("From item: " + select.getFromItem());
				System.out.println("Joins: " + select.getJoins());
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