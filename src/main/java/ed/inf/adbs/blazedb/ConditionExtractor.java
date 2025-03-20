package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.List;
import java.util.Map;

public class ConditionExtractor {

    public static void extract(Expression expression, PlainSelect select, Map<String, Expression> selectionConditions, List<Expression> joinConditions) {
        if (expression instanceof AndExpression) {
            AndExpression andExpr = (AndExpression) expression;
            extract(andExpr.getLeftExpression(), select, selectionConditions, joinConditions);
            extract(andExpr.getRightExpression(), select, selectionConditions, joinConditions);
        } else if (expression instanceof BinaryExpression) {
            categorizeCondition((BinaryExpression) expression, select, selectionConditions, joinConditions);
        }
    }

    private static void categorizeCondition(BinaryExpression condition, PlainSelect select, Map<String, Expression> selectionConditions, List<Expression> joinConditions) {
        String leftTable = getTableName(condition.getLeftExpression());
        String rightTable = getTableName(condition.getRightExpression());

        if (leftTable != null && rightTable != null && !leftTable.equals(rightTable) && !leftTable.equals("CONSTANT") && !rightTable.equals("CONSTANT")) {
            joinConditions.add(condition); // Join condition
        } else if (leftTable != null && !leftTable.equals("CONSTANT")) {
            selectionConditions.put(leftTable, mergeSelectionCondition(selectionConditions.get(leftTable), condition));
        } else if (rightTable != null && !rightTable.equals("CONSTANT")) {
            selectionConditions.put(rightTable, mergeSelectionCondition(selectionConditions.get(rightTable), condition));
        } else {
            selectionConditions.put(select.getFromItem().toString(), mergeSelectionCondition(selectionConditions.get(select.getFromItem().toString()), condition));
        }
    }

    private static String getTableName(Expression expr) {
        if (expr instanceof Column) {
            return ((Column) expr).getTable().getName();
        } else if (expr instanceof LongValue) {
            return "CONSTANT";
        }
        return null;
    }

    private static Expression mergeSelectionCondition(Expression existing, Expression newCondition) {
        return (existing == null) ? newCondition : new AndExpression(existing, newCondition);
    }

    public static boolean isJoinCondition(Expression condition, String leftTable, String rightTable) {
        if (condition instanceof BinaryExpression) {
            BinaryExpression binaryExpr = (BinaryExpression) condition;
            String left = getTableName(binaryExpr.getLeftExpression());
            String right = getTableName(binaryExpr.getRightExpression());
            return (left != null && right != null) && ((left.equals(leftTable) && right.equals(rightTable)) || (left.equals(rightTable) && right.equals(leftTable)));
        }
        return false;
    }
}
