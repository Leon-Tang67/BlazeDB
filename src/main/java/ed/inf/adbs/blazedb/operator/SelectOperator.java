package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.ExpressionEvaluator;
import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SelectOperator extends Operator {
    private Operator childOperator;
    private Expression selectionCondition;
    private List<Integer> schema;

    public SelectOperator(Operator childOperator, Expression selectionCondition) throws IOException {
        this.childOperator = childOperator;
        this.selectionCondition = selectionCondition;
        this.schema = DatabaseCatalog.getInstance("").getTableSchema(childOperator.getTableName());
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple, schema);
            if (evaluator.evaluate(selectionCondition)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public void reset() {
        childOperator.reset();
    }

    @Override
    public String getTableName() {
        return childOperator.getTableName();
    }
}
