package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.ExpressionEvaluator;
import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;
import java.util.List;


/**
 * The SelectOperator class is responsible for filtering the tuples based on the selection condition.
 * It acts as a filter on top of the child operator and only returns the tuples that satisfy the selection condition.
 *
 * The SelectOperator class contains the following methods:
 * - getNextTuple(): Retrieves the next tuple that satisfies the selection condition.
 * - reset(): Resets the iterator to the start.
 * - getTableName(): Returns the name of the table.
 * - getTableSchema(): Returns the schema of the table.
 *
 * The SelectOperator class also contains the following instance variables:
 * - childOperator: The child operator of the SelectOperator.
 * - selectionCondition: The selection condition to be applied on the tuples.
 * - schema: The schema of the table.
 */

public class SelectOperator extends Operator {
    private final Operator childOperator;
    private final Expression selectionCondition;
    private final List<String> schema;

    public SelectOperator(Operator childOperator, Expression selectionCondition) throws IOException {
        this.childOperator = childOperator;
        this.selectionCondition = selectionCondition;
        this.schema = childOperator.getTableSchema();
    }

    /**
     * Retrieves the next tuple that satisfies the selection condition.
     * @return A Tuple object representing the row of data, or NULL if EOF reached.
     */
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

    @Override
    public String getTableName() {
        return childOperator.getTableName();
    }

    @Override
    public List<String> getTableSchema() {
        return childOperator.getTableSchema();
    }
}
