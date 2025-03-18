package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;

import java.util.List;
import java.util.Map;

public class ConditionExtractor {

    public static void extract(Expression expression, Map<String, Expression> selectionConditions, List<Expression> joinConditions) {
        if (expression instanceof AndExpression) {
            AndExpression andExpr = (AndExpression) expression;
            extract(andExpr.getLeftExpression(), selectionConditions, joinConditions);
            extract(andExpr.getRightExpression(), selectionConditions, joinConditions);
        } else if (expression instanceof BinaryExpression) {
            categorizeCondition((BinaryExpression) expression, selectionConditions, joinConditions);
        }
    }

    private static void categorizeCondition(BinaryExpression condition, Map<String, Expression> selectionConditions, List<Expression> joinConditions) {
        String leftTable = getTableName(condition.getLeftExpression());
        String rightTable = getTableName(condition.getRightExpression());

        if (leftTable != null && rightTable != null && !leftTable.equals(rightTable)) {
            joinConditions.add(condition); // Join condition
        } else if (leftTable != null) {
            selectionConditions.put(leftTable, mergeSelectionCondition(selectionConditions.get(leftTable), condition));
        } else if (rightTable != null) {
            selectionConditions.put(rightTable, mergeSelectionCondition(selectionConditions.get(rightTable), condition));
        }
    }

    private static String getTableName(Expression expr) {
        if (expr instanceof Column) {
            return ((Column) expr).getTable().getName();
        }
        return null;
    }

    private static Expression mergeSelectionCondition(Expression existing, Expression newCondition) {
        return (existing == null) ? newCondition : new AndExpression(existing, newCondition);
    }

    public static boolean isJoinCondition(Expression condition, String leftTable, String rightTable) {
        if (condition instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) condition;
            String left = getTableName(equalsTo.getLeftExpression());
            String right = getTableName(equalsTo.getRightExpression());
            return (left != null && right != null) && ((left.equals(leftTable) && right.equals(rightTable)) || (left.equals(rightTable) && right.equals(leftTable)));
        }
        // TODO: Handle other types of join conditions
        return false;
    }
}
