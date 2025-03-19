package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class JoinOperator extends Operator {
    private Operator leftChild;
    private Operator rightChild;
    private Expression joinCondition;
    private List<String> schema;
    private Tuple leftTuple;

    public JoinOperator(Operator leftChild, Operator rightChild, Expression joinCondition) throws IOException {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinCondition = joinCondition;
        this.schema = new ArrayList<>();
        this.schema.addAll(leftChild.getTableSchema());
        this.schema.addAll(rightChild.getTableSchema());
        this.leftTuple = leftChild.getNextTuple();
    }

    @Override
    public Tuple getNextTuple() {
        Tuple rightTuple;
        while (leftTuple != null) {
            while ((rightTuple = rightChild.getNextTuple()) != null) {
                List<Integer> tupleValues = new ArrayList<>();
                tupleValues.addAll(leftTuple.getValues());
                tupleValues.addAll(rightTuple.getValues());
                Tuple joinedTuples = new Tuple(tupleValues);

                if (joinCondition == null) {
                    return joinedTuples; // Cross product case
                }

                ExpressionEvaluator evaluator = new ExpressionEvaluator(joinedTuples, schema);
                joinCondition.accept(evaluator);

                if (evaluator.getResult()) {
                    return joinedTuples; // Return only if the condition holds
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
    }

    @Override
    public String getTableName() {
        return leftChild.getTableName() + " JOIN " + rightChild.getTableName();
    }

    @Override
    public List<String> getTableSchema() {
        return schema;
    }
}
