package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;

import java.util.List;

public class ExpressionEvaluator extends ExpressionVisitorAdapter {
    private Tuple tuple;
    private List<String> schema;
    private boolean result;

    public ExpressionEvaluator(Tuple tuple, List<String> schema) {
        this.tuple = tuple;
        this.schema = schema;
    }

    @Override
    public void visit(AndExpression andExpression) {
        ExpressionEvaluator leftEvaluator = new ExpressionEvaluator(tuple, schema);
        ExpressionEvaluator rightEvaluator = new ExpressionEvaluator(tuple, schema);

        andExpression.getLeftExpression().accept(leftEvaluator);
        andExpression.getRightExpression().accept(rightEvaluator);

        result = leftEvaluator.getResult() && rightEvaluator.getResult();
    }

    @Override
    public void visit(LongValue longValue) {
        // Store the value for further comparisons
        result = (longValue.getValue() != 0); // Treat nonzero values as true
    }

    @Override
    public void visit(Column column) {
        String columnName = column.getColumnName();
        int index = schema.indexOf(columnName);
        if (index != -1) {
            result = true; // Just resolving column references
        }
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        Object leftValue = evaluateExpression(equalsTo.getLeftExpression());
        Object rightValue = evaluateExpression(equalsTo.getRightExpression());

        if (leftValue instanceof Number && rightValue instanceof Number) {
            result = ((Number) leftValue).doubleValue() == ((Number) rightValue).doubleValue();
        } else {
            result = leftValue.toString().equals(rightValue.toString());
        }
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        Object leftValue = evaluateExpression(notEqualsTo.getLeftExpression());
        Object rightValue = evaluateExpression(notEqualsTo.getRightExpression());

        if (leftValue instanceof Number && rightValue instanceof Number) {
            result = ((Number) leftValue).doubleValue() != ((Number) rightValue).doubleValue();
        } else {
            result = !leftValue.toString().equals(rightValue.toString());
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        Object leftValue = evaluateExpression(greaterThan.getLeftExpression());
        Object rightValue = evaluateExpression(greaterThan.getRightExpression());

        if (leftValue instanceof Number && rightValue instanceof Number) {
            result = ((Number) leftValue).doubleValue() > ((Number) rightValue).doubleValue();
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        Object leftValue = evaluateExpression(greaterThanEquals.getLeftExpression());
        Object rightValue = evaluateExpression(greaterThanEquals.getRightExpression());

        if (leftValue instanceof Number && rightValue instanceof Number) {
            result = ((Number) leftValue).doubleValue() >= ((Number) rightValue).doubleValue();
        }
    }

    @Override
    public void visit(MinorThan minorThan) {
        Object leftValue = evaluateExpression(minorThan.getLeftExpression());
        Object rightValue = evaluateExpression(minorThan.getRightExpression());

        if (leftValue instanceof Number && rightValue instanceof Number) {
            result = ((Number) leftValue).doubleValue() < ((Number) rightValue).doubleValue();
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        Object leftValue = evaluateExpression(minorThanEquals.getLeftExpression());
        Object rightValue = evaluateExpression(minorThanEquals.getRightExpression());

        if (leftValue instanceof Number && rightValue instanceof Number) {
            result = ((Number) leftValue).doubleValue() <= ((Number) rightValue).doubleValue();
        }
    }

    @Override
    public void visit(Multiplication multiplication) {
        Object leftValue = evaluateExpression(multiplication.getLeftExpression());
        Object rightValue = evaluateExpression(multiplication.getRightExpression());

        if (leftValue instanceof Number && rightValue instanceof Number) {
            result = true; // Just a placeholder, SUM aggregates will be handled separately.
        }
    }

    private Object evaluateExpression(Expression expr) {
        if (expr instanceof Column) {
            String columnName = ((Column) expr).getColumnName();
            int index = schema.indexOf(columnName);
            if (index != -1) {
                return tuple.getValue(index);
            }
        } else if (expr instanceof LongValue) {
            return ((LongValue) expr).getValue();
        }
        return null;
    }

    public boolean getResult() {
        return result;
    }
}
