package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.ExpressionEvaluator;
import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelectOperator extends Operator {
    private Operator childOperator;
    private Expression selectionCondition;
    private Map<String, List<String>> schema;

    public SelectOperator(Operator childOperator, Expression selectionCondition) throws IOException {
        this.childOperator = childOperator;
        this.selectionCondition = selectionCondition;
        List<String> schema = DatabaseCatalog.getInstance("").getTableSchema(childOperator.getTableName());
        this.schema = new HashMap<>();
        this.schema.put(childOperator.getTableName(), schema);
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            Map<String, Tuple> tupleMap = new HashMap<>();
            tupleMap.put(childOperator.getTableName(), tuple);
            ExpressionEvaluator evaluator = new ExpressionEvaluator(tupleMap, schema);
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
}
