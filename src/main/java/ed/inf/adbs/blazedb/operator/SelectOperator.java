package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.DatabaseCatalog;
import net.sf.jsqlparser.expression.Expression;
import java.io.IOException;
import java.util.List;

public class SelectOperator extends Operator {
    private ScanOperator childOperator;
    private Expression condition;
    private List<String> schema;

    public SelectOperator(ScanOperator childOperator, Expression condition) throws IOException {
        this.childOperator = childOperator;
        this.condition = condition;
        this.schema = DatabaseCatalog.getInstance("").getTableSchema(childOperator.getTableName());
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple, schema);
            condition.accept(evaluator);
            if (evaluator.getResult()) {
                return tuple;
            }
        }
        return null;  // No more tuples match the condition
    }

    @Override
    public void reset() {
        childOperator.reset();
    }
}
