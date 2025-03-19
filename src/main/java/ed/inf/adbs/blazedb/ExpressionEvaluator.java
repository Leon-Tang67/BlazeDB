package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import java.util.List;
import java.util.Map;

public class ExpressionEvaluator extends ExpressionVisitorAdapter {
    private Map<String, Tuple> tupleMap;
    private Map<String, List<String>> schemaMap;
    private boolean result;
    private int value;

    public ExpressionEvaluator(Map<String, Tuple> tupleMap, Map<String, List<String>> schemaMap) {
        this.tupleMap = tupleMap;
        this.schemaMap = schemaMap;
        this.result = false;
    }

    public boolean getResult() {
        return result;
    }

    @Override
    public void visit(AndExpression andExpr) {
        ExpressionEvaluator leftEval = new ExpressionEvaluator(tupleMap, schemaMap);
        andExpr.getLeftExpression().accept(leftEval);

        ExpressionEvaluator rightEval = new ExpressionEvaluator(tupleMap, schemaMap);
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

    private void evaluateComparison(BinaryExpression expr, String operator) {
        ExpressionEvaluator leftEval = new ExpressionEvaluator(tupleMap, schemaMap);
        expr.getLeftExpression().accept(leftEval);

        ExpressionEvaluator rightEval = new ExpressionEvaluator(tupleMap, schemaMap);
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
        String tableName = column.getTable().getName();
        String columnName = column.getColumnName();

        if (schemaMap.containsKey(tableName)) {
            value = tupleMap.get(tableName).getValue(schemaMap.get(tableName).indexOf(columnName));
        } else {
            throw new RuntimeException("Column " + columnName + " not found in schema.");
        }
    }

    @Override
    public void visit(LongValue longValue) {
        value = (int) longValue.getValue();
    }

    @Override
    public void visit(Multiplication multiplication) {
        ExpressionEvaluator leftEval = new ExpressionEvaluator(tupleMap, schemaMap);
        multiplication.getLeftExpression().accept(leftEval);

        ExpressionEvaluator rightEval = new ExpressionEvaluator(tupleMap, schemaMap);
        multiplication.getRightExpression().accept(rightEval);

        value = leftEval.value * rightEval.value;
    }

//    @Override
//    public void visit(Function function) {
//        if (function.getName().equalsIgnoreCase("SUM")) {
//            if (function.getParameters() != null && function.getParameters().getExpressions().size() == 1) {
//                Expression param = function.getParameters().getExpressions().get(0);
//                param.accept(this);
//            } else {
//                throw new RuntimeException("SUM function must have exactly one parameter.");
//            }
//        } else {
//            throw new RuntimeException("Unsupported function: " + function.getName());
//        }
//    }

//    @Override
//    public void visit(NullValue nullValue) {}
}
