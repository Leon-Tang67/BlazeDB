package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SortOperator extends Operator{
    private final Operator childOperator;
    private final List<Integer> columnIndexes;
    private final List<String> schema;
    private final List<Tuple> tuplesList;
    private int currentTupleIndex;

    public SortOperator(Operator childOperator, List<OrderByElement> orderByElements) {
        this.childOperator = childOperator;
        this.schema = childOperator.getTableSchema();
        this.tuplesList = new ArrayList<>();
        this.currentTupleIndex = 0;

        // Convert column names to indexes
        columnIndexes = new ArrayList<>();
        for (OrderByElement element : orderByElements) {
            Column column = ((Column) element.getExpression());
            String columnFullName = column.getFullyQualifiedName();
            columnIndexes.add(schema.indexOf(columnFullName));
        }
    }

    @Override
    public Tuple getNextTuple() {
        if (tuplesList.isEmpty()) {
            Tuple tuple;
            while ((tuple = childOperator.getNextTuple()) != null) {
                tuplesList.add(tuple);
            }

            // Sort tuples
            Collections.sort(tuplesList, new Comparator<Tuple>() {
                @Override
                public int compare(Tuple t1, Tuple t2) {
                    for (int i = 0; i < columnIndexes.size(); i++) {
                        int index = columnIndexes.get(i);
                        int value1 = t1.getValue(index);
                        int value2 = t2.getValue(index);
                        if (value1 != value2) {
                            return Integer.compare(value1, value2);
                        }
                    }
                    return 0;
                }
            });
        }

        if (currentTupleIndex < tuplesList.size()) {
            return tuplesList.get(currentTupleIndex++);
        }
        return null;
    }

    @Override
    public void reset() {
        childOperator.reset();
        tuplesList.clear();
        currentTupleIndex = 0;
    }

    @Override
    public String getTableName() {
        return childOperator.getTableName();
    }

    @Override
    public List<String> getTableSchema() {
        return schema;
    }
}
