package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DuplicateEliminationOperator extends Operator{
    private Operator childOperator;
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
