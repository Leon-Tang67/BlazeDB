package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The DuplicateEliminationOperator class is responsible for removing duplicate tuples from the input.
 * It takes a child operator as input and removes duplicate tuples from the output of the child operator.
 *
 * The DuplicateEliminationOperator class extends the Operator class and implements the following methods:
 * - getNextTuple(): Retrieves the next tuple with duplicates removed.
 * - reset(): Resets the iterator to the start.
 * - getTableName(): Returns the name of the table.
 * - getTableSchema(): Returns the schema of the table.
 *
 * The DuplicateEliminationOperator class also contains the following instance variables:
 * - childOperator: The child operator of the DuplicateEliminationOperator.
 * - tupleSet: A set to store unique tuples.
 */

public class DuplicateEliminationOperator extends Operator{
    private final Operator childOperator;
    private Set<Tuple> tupleSet;

    public DuplicateEliminationOperator(Operator childOperator) {
        this.childOperator = childOperator;
        this.tupleSet = new HashSet<>();
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            if (!tupleSet.contains(tuple)) {
                tupleSet.add(tuple);
                return tuple;
            }
        }
        return null;
    }

    @Override
    public void reset() {
        childOperator.reset();
        tupleSet = new HashSet<>();
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
