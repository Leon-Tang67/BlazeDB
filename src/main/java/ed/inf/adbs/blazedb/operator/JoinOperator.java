package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

/**
 * The JoinOperator class is responsible for performing the join operation between two tables.
 * It takes two child operators as input and a join condition to perform the join operation.
 * <br><br>
 * The JoinOperator class contains the following methods:<br>
 * - getNextTuple(): Retrieves the next tuple that satisfies the join condition.<br>
 * - reset(): Resets the iterator to the start.<br>
 * - getTableName(): Returns the name of the table.<br>
 * - getTableSchema(): Returns the schema of the table.
 * <br><br>
 * The JoinOperator class also contains the following instance variables:<br>
 * - leftChild: The left child operator of the JoinOperator.<br>
 * - rightChild: The right child operator of the JoinOperator.<br>
 * - joinCondition: The join condition to be applied on the tuples.<br>
 * - schema: The schema of the joined table.<br>
 * - leftTuple: The current tuple from the left child operator.
 */

public class JoinOperator extends Operator {
    private final Operator leftChild;
    private final Operator rightChild;
    private final Expression joinCondition;
    private final List<String> schema;
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

    /**
     * Retrieves the next tuple that satisfies the join condition.
     * @return A Tuple object representing the joined row of data, or NULL if EOF reached.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple rightTuple;
        while (leftTuple != null) {
            while ((rightTuple = rightChild.getNextTuple()) != null) {
                // Combine the values of the left and right tuples
                List<Integer> tupleValues = new ArrayList<>();
                tupleValues.addAll(leftTuple.getValues());
                tupleValues.addAll(rightTuple.getValues());
                Tuple joinedTuples = new Tuple(tupleValues);

                if (joinCondition == null) {
                    return joinedTuples; // Cross product case
                }

                // Evaluate the join condition
                ExpressionEvaluator evaluator = new ExpressionEvaluator(joinedTuples, schema);
                joinCondition.accept(evaluator);
                if (evaluator.getResult()) {
                    return joinedTuples; // Return only if the condition holds
                }
            }

            rightChild.reset(); // Reset right child for next left tuple
            leftTuple = leftChild.getNextTuple();
        }
        return null;
    }

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
    }

    /**
     * Returns the name of the two tables with JOIN keyword in between.
     * @return The name of the table.
     */
    @Override
    public String getTableName() {
        return leftChild.getTableName() + " JOIN " + rightChild.getTableName();
    }

    @Override
    public List<String> getTableSchema() {
        return schema;
    }
}
