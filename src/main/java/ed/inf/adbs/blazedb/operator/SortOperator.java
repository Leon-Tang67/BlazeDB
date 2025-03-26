package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * The SortOperator class is responsible for sorting the tuples based on the ORDER BY clause.
 * It takes a child operator as input and sorts the tuples based on the specified columns.
 *
 * The SortOperator class contains the following methods:
 * - getNextTuple(): Retrieves the next tuple that satisfies the ORDER BY clause.
 * - reset(): Resets the iterator to the start.
 * - getTableName(): Returns the name of the table.
 * - getTableSchema(): Returns the schema of the table.
 *
 * The SortOperator class also contains the following instance variables:
 * - childOperator: The child operator of the SortOperator.
 * - columnIndexes: A list of column indexes to be sorted.
 * - schema: The schema of the table.
 * - tuplesList: A list of tuples to be sorted.
 * - currentTupleIndex: The index of the current tuple in the sorted list.
 */
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

        // Convert column names in the ORDER BY clause to indexes for sorting
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

            // Read all tuples from the child operator. This is the blocking point
            while ((tuple = childOperator.getNextTuple()) != null) {
                tuplesList.add(tuple);
            }

            // Sort tuples with customized comparator based on ORDER BY columns
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

        // Return the next tuple from the sorted list
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
