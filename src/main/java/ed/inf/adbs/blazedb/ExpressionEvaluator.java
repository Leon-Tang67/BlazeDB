package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import java.util.List;

/**
 * The ExpressionEvaluator class is responsible for evaluating the expressions in the WHERE clause.
 * It evaluates the expressions based on the tuple values and the schema of the table.
 *
 * The ExpressionEvaluator class contains the following methods:
 * - getResult(): Returns the result of the evaluation.
 * - getValue(): Returns the value of the evaluated expression.
 * - evaluateComparison(): Evaluates the comparison expressions.
 * - The other methods are overridden from the ExpressionVisitorAdapter class to visit different types of expressions.
 *
 * The ExpressionEvaluator class also contains the following instance variables:
 * - tuple: The tuple containing the values of the table.
 * - schema: The schema of the table.
 * - result: The result of the evaluation.
 * - value: The value of the evaluated expression.
 */
public class ExpressionEvaluator extends ExpressionVisitorAdapter {
    private final Tuple tuple;
    private final List<String> schema;
    private boolean result;
    private int value;

    public ExpressionEvaluator(Tuple tuple, List<String> schema) {
        this.tuple = tuple;
        this.schema = schema;
        this.result = false;
    }

    public boolean getResult() {
        return result;
    }

    public int getValue() {
        return value;
    }

    // Recursively evaluate the left and right expressions of the AND expression
    @Override
    public void visit(AndExpression andExpr) {
        ExpressionEvaluator leftEval = new ExpressionEvaluator(tuple, schema);
        andExpr.getLeftExpression().accept(leftEval);

        ExpressionEvaluator rightEval = new ExpressionEvaluator(tuple, schema);
        andExpr.getRightExpression().accept(rightEval);

        result = leftEval.getResult() && rightEval.getResult();
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        evaluateComparison(equalsTo, "=");
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        evaluateComparison(notEqualsTo, "!=");
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        evaluateComparison(greaterThan, ">");
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        evaluateComparison(greaterThanEquals, ">=");
    }

    @Override
    public void visit(MinorThan minorThan) {
        evaluateComparison(minorThan, "<");
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        evaluateComparison(minorThanEquals, "<=");
    }

    // Evaluate the comparison expressions based on the operator
    private void evaluateComparison(BinaryExpression expr, String operator) {
        ExpressionEvaluator leftEval = new ExpressionEvaluator(tuple, schema);
        expr.getLeftExpression().accept(leftEval);

        ExpressionEvaluator rightEval = new ExpressionEvaluator(tuple, schema);
        expr.getRightExpression().accept(rightEval);

        int leftValue = leftEval.value;
        int rightValue = rightEval.value;

        switch (operator) {
            case "=":
                result = leftValue == rightValue;
                break;
            case "!=":
                result = leftValue != rightValue;
                break;
            case ">":
                result = leftValue > rightValue;
                break;
            case ">=":
                result = leftValue >= rightValue;
                break;
            case "<":
                result = leftValue < rightValue;
                break;
            case "<=":
                result = leftValue <= rightValue;
                break;
        }
    }

    @Override
    public void visit(Column column) {
        String columnFullName = column.getFullyQualifiedName();
        if (schema.contains(columnFullName)) {
            value = tuple.getValue(schema.indexOf(columnFullName));
        } else {
            throw new RuntimeException("Column " + columnFullName + " not found in schema.");
        }
    }

    @Override
    public void visit(LongValue longValue) {
        value = (int) longValue.getValue();
    }

    @Override
    public void visit(Multiplication multiplication) {
        ExpressionEvaluator leftEval = new ExpressionEvaluator(tuple, schema);
        multiplication.getLeftExpression().accept(leftEval);

        ExpressionEvaluator rightEval = new ExpressionEvaluator(tuple, schema);
        multiplication.getRightExpression().accept(rightEval);

        value = leftEval.value * rightEval.value;
    }
}
