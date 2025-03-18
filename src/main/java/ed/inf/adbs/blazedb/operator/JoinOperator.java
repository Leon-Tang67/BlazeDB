package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class JoinOperator extends Operator {
    private Operator leftChild;
    private Operator rightChild;
    private Expression condition;
    private Tuple leftTuple;
    private List<String> leftSchema;
    private List<String> rightSchema;

    public JoinOperator(Operator leftChild, Operator rightChild, Expression condition) throws IOException {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.condition = condition;
        this.leftTuple = leftChild.getNextTuple();
        this.leftSchema = DatabaseCatalog.getInstance("").getTableSchema(leftChild.getTableName());
        this.rightSchema = DatabaseCatalog.getInstance("").getTableSchema(rightChild.getTableName());
    }

    @Override
    public Tuple getNextTuple() {
        Tuple rightTuple;
        while (leftTuple != null) {
            while ((rightTuple = rightChild.getNextTuple()) != null) {
                List<Integer> joinedValues = new ArrayList<>(leftTuple.getValues());
                joinedValues.addAll(rightTuple.getValues());
                Tuple joinedTuple = new Tuple(joinedValues);

                if (condition == null) {
                    return joinedTuple; // Cross product case
                }

                List<String> fullSchema = new ArrayList<>(leftSchema);
                fullSchema.addAll(rightSchema);
                ExpressionEvaluator evaluator = new ExpressionEvaluator(joinedTuple, fullSchema);
                condition.accept(evaluator);

                if (evaluator.getResult()) {
                    return joinedTuple; // Return only if the condition holds
                }
            }

            rightChild.reset(); // Reset right child for next left tuple
            leftTuple = leftChild.getNextTuple();
        }
        return null; // No more tuples
    }

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
        leftTuple = leftChild.getNextTuple();
    }

    @Override
    public String getTableName() {
        return ""; // Not relevant for joins
    }
}
