package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.ExpressionEvaluator;
import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;
import java.util.List;

public class SelectOperator extends Operator {
    private Operator childOperator;
    private Expression selectionCondition;
    private List<String> schema;

    public SelectOperator(Operator childOperator, Expression selectionCondition) throws IOException {
        this.childOperator = childOperator;
        this.selectionCondition = selectionCondition;
        this.schema = childOperator.getTableSchema();
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple, schema);
            selectionCondition.accept(evaluator);
            if (evaluator.getResult()) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public void reset() {
        childOperator.reset();
    }

    public Operator getChild() {
        return childOperator;
    }

    @Override
    public String getTableName() {
        return childOperator.getTableName();
    }

    @Override
    public List<String> getTableSchema() {
        return childOperator.getTableSchema();
    }
}
