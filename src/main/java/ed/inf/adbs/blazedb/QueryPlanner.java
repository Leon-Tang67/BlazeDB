package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.IOException;
import java.util.*;

/**
 * The QueryPlanner class is responsible for generating the query plan for the provided SQL query.
 * It extracts the selection and join conditions from the WHERE clause and builds the operator tree accordingly.
 * The operator tree is then used to execute the query and write the result to the output file.
 *
 * The QueryPlanner class contains the following methods:
 * - generatePlan(): Generates the query plan for the provided SQL query.
 * - extractConditions(Expression where): Extracts the selection and join conditions from the WHERE clause.
 * - buildTableScans(): Builds the table scan operators for the FROM clause tables.
 *                      If selection conditions are present, a SelectOperator is added on top of the ScanOperator.
 * - buildOperatorTree(): Builds the operator tree based on the extracted conditions and table scans.
 * - findJoinCondition(String leftTable, String rightTable): Finds the join condition between two tables.
 * - getFromTables(): Returns a list of table names from the FROM clause.
 *
 * The QueryPlanner class also contains the following instance variables:
 * - select: The PlainSelect object representing the SQL query.
 * - tableScansMapping: A mapping of table names to table ScanOperators or SelectOperator.
 * - joinConditions: A list of join conditions extracted from the WHERE clause.
 * - selectionConditions: A mapping of table names to selection conditions.
 */

 public class QueryPlanner {
    private final PlainSelect select;
    private final Map<String, Operator> tableScansMapping;
    private final List<Expression> joinConditions;
    private final Map<String, Expression> selectionConditions;

    public QueryPlanner(PlainSelect select) {
        this.select = select;
        this.tableScansMapping = new HashMap<>();
        this.joinConditions = new ArrayList<>();
        this.selectionConditions = new HashMap<>();
    }

    /**
     * Generates the query plan for the provided SQL query.
     * Extracts the selection and join conditions from the WHERE clause
     * and builds the operator tree accordingly.
     *
     * @return The root operator of the operator tree.
     */
    public Operator generatePlan() throws IOException {
        extractConditions(select.getWhere());
        buildTableScans();
        return buildOperatorTree();
    }

    /**
     * Extracts the selection and join conditions from the WHERE clause.
     *
     * @param where The WHERE clause expression.
     */
    private void extractConditions(Expression where) {
        if (where == null) return; // No conditions to extract
        // Call ConditionExtractor to extract selection and join conditions
        ConditionExtractor.extract(where, this.select, selectionConditions, joinConditions);
    }

    /**
     * Builds the table scan operators for the FROM clause tables.
     * If selection conditions are present, a SelectOperator is added on top of the ScanOperator.
     */
    private void buildTableScans() throws IOException {
        // Create ScanOperators for each table in the FROM clause
        for (String table : getFromTables()) {
            Operator scan = new ScanOperator(table);
            if (selectionConditions.containsKey(table)) {
                scan = new SelectOperator(scan, selectionConditions.get(table));
            }
            tableScansMapping.put(table, scan);
        }
    }

    /**
     * Builds the operator tree based on the extracted conditions and table scans.
     *
     * @return The root operator of the operator tree.
     */
    private Operator buildOperatorTree() throws IOException {
        // Ensure left deep join tree, starting with the first table
        Iterator<String> tableNames = getFromTables().iterator();
        Operator root = tableScansMapping.get(tableNames.next());

        // Build the join tree by adding JoinOperators with the appropriate join conditions
        while (tableNames.hasNext()) {
            String nextTableName = tableNames.next();
            Operator right = tableScansMapping.get(nextTableName);
            Expression joinCondition = findJoinCondition(root.getTableName(), nextTableName);
            root = new JoinOperator(root, right, joinCondition); //joinCondition can be null indicating cross product
        }

        // Add ProjectOperator on top if the first select item is not AllColumns (there is/are projection condition(s))
        // or if the first item is AllColumn but there is/are SUM clause(s) in the select list
        if (!(select.getSelectItems().get(0).getExpression() instanceof AllColumns) ||
                select.getSelectItems().stream().anyMatch(item -> item.toString().contains("SUM"))) {
            root = new ProjectOperator(root, select);
        }

        // Add SumOperator on top if there is a SUM clause in the select list or if there is a GROUP BY clause
        if (select.getSelectItems().stream().anyMatch(item -> item.toString().contains("SUM")) || select.getGroupBy() != null) {
            if (select.getGroupBy() != null) {
                root = new SumOperator(root, select.getSelectItems(), select.getGroupBy().getGroupByExpressionList());
            } else {
                root = new SumOperator(root, select.getSelectItems(), null);
            }
        }

        // Add SortOperator on top if there is an ORDER BY clause
        if (select.getOrderByElements() != null) {
            root = new SortOperator(root, select.getOrderByElements());
        }

        // Add DuplicateEliminationOperator on top if there is a DISTINCT clause
        if (select.getDistinct() != null) {
            root = new DuplicateEliminationOperator(root);
        }

        return root;
    }

    /**
     * Finds the join condition between two tables.
     *
     * @param leftTable  The name of the left table.
     * @param rightTable The name of the right table.
     * @return The join condition between the two tables.
     */
    private Expression findJoinCondition(String leftTable, String rightTable) {
        // Iterate through the join conditions to find the one that matches the two tables
        List<Expression> conditions = new ArrayList<>();
        for (Expression cond : joinConditions) {
            if (ConditionExtractor.isJoinCondition(cond, leftTable, rightTable)) {
                conditions.add(cond);
            }
        }

        // If no join condition is found, check if the table is a joined table with JOIN keyword
        // and extract the join condition based on the table names from the joined table
        // This is particularly useful if there are multiple joins between the multiple tables
        // The firstly joined tables will be on the left of the join tree and the joined table's name will have JOIN keyword
        if (conditions.isEmpty() && leftTable.contains("JOIN")) {
            String[] joinedTables = leftTable.split(" JOIN ");
            for (String table : joinedTables) {
                Expression condition = findJoinCondition(table, rightTable);
                if (condition != null) {
                    conditions.add(condition);
                }
            }
        }

        // Return if there is no join condition found
        if (conditions.isEmpty()) {
            return null;
        }

        // Combine multiple join conditions into a single AndExpression for future use
        Expression combinedConditions = conditions.get(0);
        for (int i = 1; i < conditions.size(); i++) {
            combinedConditions = new AndExpression(combinedConditions, conditions.get(i));
        }
        return combinedConditions;
    }

    /**
     * Returns a list of table names from the FROM clause.
     *
     * @return A list of table names.
     */
    private List<String> getFromTables() {
        List<String> tableNames = new ArrayList<>();
        tableNames.add(select.getFromItem().toString());
        if (select.getJoins() != null) {
            select.getJoins().forEach(join -> tableNames.add(join.toString()));
        }
        return tableNames;
    }
}
