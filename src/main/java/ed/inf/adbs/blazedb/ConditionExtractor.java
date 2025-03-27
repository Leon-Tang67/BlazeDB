package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.List;
import java.util.Map;


/**
 * Extracts selection and join conditions from a given expression.<br>
 * Selection conditions are stored in a map where the key is the table name and the value is the selection condition.
 * Join conditions are stored in a list.
 * <br><br>
 * The ConditionExtractor class contains the following methods:<br>
 * - extract(): Extracts selection and join conditions from the given expression.<br>
 * - categorizeCondition(): Categorizes the condition as a selection or join condition.<br>
 * - mergeSelectionCondition(): Merges the existing selection condition with the new condition.<br>
 * - isJoinCondition(): Checks if the condition is a join condition between the given tables.<br>
 * - getTableName(): Returns the table name of the given expression.
 */

public class ConditionExtractor {

    /**
     * Extracts selection and join conditions from the given expression.
     * @param expression The expression to extract conditions from.
     * @param select The select statement to extract conditions for.
     * @param selectionConditions The map to store selection conditions.
     * @param joinConditions The list to store join conditions.
     *
     * @Description:
     * Recursively extract conditions if the expression is an AndExpression.
     * Otherwise, categorize the condition as a selection or join condition.
     */
    public static void extract(Expression expression, PlainSelect select, Map<String, Expression> selectionConditions, List<Expression> joinConditions) {
        if (expression instanceof AndExpression) {
            AndExpression andExpr = (AndExpression) expression;
            extract(andExpr.getLeftExpression(), select, selectionConditions, joinConditions);
            extract(andExpr.getRightExpression(), select, selectionConditions, joinConditions);
        } else if (expression instanceof BinaryExpression) {
            categorizeCondition((BinaryExpression) expression, select, selectionConditions, joinConditions);
        }
    }

    /**
     * Categorizes the condition as a selection or join condition.
     * @param condition The condition to categorize.
     * @param select The select statement to categorize the condition for.
     * @param selectionConditions The map to store selection conditions.
     * @param joinConditions The list to store join conditions.
     *
     * @Description:
     * Categorize the condition as a selection or join condition and store it accordingly.<br>
     * - If both sides of the expression are tables, add it as a join condition.<br>
     * - If one side of the expression is a constant, add it as a select condition to the table on the other side.<br>
     * - If both sides of the expression are constants, add it as a selection condition to the first table in the FROM clause.
     */
    private static void categorizeCondition(BinaryExpression condition, PlainSelect select, Map<String, Expression> selectionConditions, List<Expression> joinConditions) {
        String leftTable = getTableName(condition.getLeftExpression());
        String rightTable = getTableName(condition.getRightExpression());

        if (leftTable != null && rightTable != null && !leftTable.equals(rightTable) && !leftTable.equals("CONSTANT") && !rightTable.equals("CONSTANT")) {
            joinConditions.add(condition);
        } else if (leftTable != null && !leftTable.equals("CONSTANT")) {
            selectionConditions.put(leftTable, mergeSelectionCondition(selectionConditions.get(leftTable), condition));
        } else if (rightTable != null && !rightTable.equals("CONSTANT")) {
            selectionConditions.put(rightTable, mergeSelectionCondition(selectionConditions.get(rightTable), condition));
        } else {
            selectionConditions.put(select.getFromItem().toString(), mergeSelectionCondition(selectionConditions.get(select.getFromItem().toString()), condition));
        }
    }

    /**
     * Merges the existing selection condition with the new condition.
     * @param existing The existing selection condition.
     * @param newCondition The new condition to merge.
     * @return The merged selection condition.
     */
    private static Expression mergeSelectionCondition(Expression existing, Expression newCondition) {
        return (existing == null) ? newCondition : new AndExpression(existing, newCondition);
    }

    /**
     * Checks if the condition is a join condition between the given tables.
     * @param condition The condition to check.
     * @param leftTable The left table name.
     * @param rightTable The right table name.
     * @return True if the condition is a join condition between the given tables, false otherwise.
     */
    public static boolean isJoinCondition(Expression condition, String leftTable, String rightTable) {
        if (condition instanceof BinaryExpression) {
            BinaryExpression binaryExpr = (BinaryExpression) condition;
            String left = getTableName(binaryExpr.getLeftExpression());
            String right = getTableName(binaryExpr.getRightExpression());
            return ((left.equals(leftTable) && right.equals(rightTable)) || (left.equals(rightTable) && right.equals(leftTable)));
        }
        return false;
    }

    /**
     * Returns the table name of the given expression.
     * @param expr The expression to get the table name from.
     * @return The table name of the expression.
     */
    private static String getTableName(Expression expr) {
        if (expr instanceof Column) {
            return ((Column) expr).getTable().getName();
        } else if (expr instanceof LongValue) {
            return "CONSTANT";
        }
        return null;
    }
}
