package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SortOperator extends Operator{
    private Operator childOperator;
    private List<Integer> columnIndexes;
    private List<String> schema;
    private int[] sortIndexes;
    private List<Tuple> tuples;
    private int currentTupleIndex;

    public SortOperator(Operator childOperator, List<OrderByElement> orderByElements) {
        this.childOperator = childOperator;
        this.schema = childOperator.getTableSchema();
        this.tuples = new ArrayList<>();
        this.currentTupleIndex = 0;

        // Convert column names to indexes
        columnIndexes = new ArrayList<>();
        for (OrderByElement element : orderByElements) {
            Column column = ((Column) element.getExpression());
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName();
            String columnFullName = tableName + "." + columnName;
            columnIndexes.add(schema.indexOf(columnFullName));
        }

        // Convert order by elements to indexes and orders
        sortIndexes = new int[orderByElements.size()];
        for (int i = 0; i < orderByElements.size(); i++) {
            OrderByElement element = orderByElements.get(i);
            Column column = ((Column) element.getExpression());
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName();
            String columnFullName = tableName + "." + columnName;
            sortIndexes[i] = schema.indexOf(columnFullName);
        }
    }

    @Override
    public Tuple getNextTuple() {
        if (tuples.isEmpty()) {
            Tuple tuple;
            while ((tuple = childOperator.getNextTuple()) != null) {
                tuples.add(tuple);
            }

            // Sort tuples
            Collections.sort(tuples, new Comparator<Tuple>() {
                @Override
                public int compare(Tuple t1, Tuple t2) {
                    for (int i = 0; i < sortIndexes.length; i++) {
                        int index = sortIndexes[i];
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

        if (currentTupleIndex < tuples.size()) {
            return tuples.get(currentTupleIndex++);
        }
        return null;
    }

    @Override
    public void reset() {
        childOperator.reset();
        tuples.clear();
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
