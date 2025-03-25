package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.*;

public class SumOperator extends Operator {
    private Operator childOperator;
    private List<String> schema;
    private List<SelectItem<?>> selectedColumns;
    private List<Integer> sumExpIndexes;
    private ExpressionList groupByColumns;
    private List<Integer> groupKey;
    private Map<List<Integer>, List<Integer>> groupColToTupleAndSumMapping;
    private Iterator<Map.Entry<List<Integer>, List<Integer>>> groupIterator;

    public SumOperator(Operator childOperator, List<SelectItem<?>> selectedColumns, ExpressionList groupByColumns) {
        this.childOperator = childOperator;
        this.schema = childOperator.getTableSchema();
        this.selectedColumns = selectedColumns;
        this.sumExpIndexes = new ArrayList<>();
        this.groupByColumns = groupByColumns;
        this.groupKey = new ArrayList<>();
        this.groupColToTupleAndSumMapping = new HashMap<>();
        this.groupIterator = null;

        int selectedColumnsIndex = 0;
        for (SelectItem<?> item : selectedColumns) {
            if (item.getExpression() instanceof Column) {
                selectedColumnsIndex++;
            } else if (item.toString().contains("SUM")) {
                sumExpIndexes.add(selectedColumnsIndex++);
            }
        }
    }

    @Override
    public Tuple getNextTuple() {
        if (groupIterator == null) {
            computeGroupSums();
            groupIterator = groupColToTupleAndSumMapping.entrySet().iterator();
        }

        if (groupIterator.hasNext()) {
            Map.Entry<List<Integer>, List<Integer>> entry = groupIterator.next();
            List<Integer> combinedTuple = new ArrayList<>();
            for (int i = 0; i < selectedColumns.size(); i++) {
                if (selectedColumns.get(i).getExpression() instanceof Column) {
                    String columnName = ((Column) selectedColumns.get(i).getExpression()).getFullyQualifiedName();
                    int columnIndex = childOperator.getTableSchema().indexOf(columnName);
                    combinedTuple.add(entry.getValue().get(columnIndex));
                } else if (selectedColumns.get(i).toString().contains("SUM")) {
                    int childSchemaSize = childOperator.getTableSchema().size();
                    int sumIndex = 0;
                    while (i < selectedColumns.size()) {
                        combinedTuple.add(entry.getValue().get(childSchemaSize+sumIndex));
                        i++;
                        sumIndex++;
                    }
                }
            }
            return new Tuple(combinedTuple);
        } else {
            return null;
        }
    }

    private void computeGroupSums() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            int tupleLength = tuple.getValues().size();
            if (groupByColumns != null) {
                groupKey = new ArrayList<>();
                for (int i = 0; i < groupByColumns.size(); i++) {
                    Column column = (Column) groupByColumns.get(i);
                    String columnFullName = column.getFullyQualifiedName();
                    groupKey.add(tuple.getValue(schema.indexOf(columnFullName)));
                }
                if (!groupColToTupleAndSumMapping.containsKey(groupKey)) {
                    groupColToTupleAndSumMapping.put(groupKey, new ArrayList<>(tuple.getValues()));
                }
            } else {
                if (groupColToTupleAndSumMapping.keySet().isEmpty()) {
                    groupKey = Collections.singletonList(0);
                    groupColToTupleAndSumMapping.put(groupKey, new ArrayList<>(tuple.getValues()));
                }
            }
            for (int i = 0; i < sumExpIndexes.size(); i++) {
                int currentIndex = sumExpIndexes.get(i);
                ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple, schema);
                selectedColumns.get(currentIndex).accept(evaluator);

                int sumValue = evaluator.getValue();

                List<Integer> existingValue = groupColToTupleAndSumMapping.get(groupKey);
                if (existingValue.size() <= i+tupleLength) {
                    existingValue.add(0);
                }
                existingValue.set(i+tupleLength, existingValue.get(i+tupleLength) + sumValue);
            }
        }
    }

    @Override
    public void reset() {
        childOperator.reset();
        groupColToTupleAndSumMapping.clear();
        groupIterator = null;
    }

    @Override
    public String getTableName() {
        return childOperator.getTableName();
    }

    @Override
    public List<String> getTableSchema() {
        for (SelectItem<?> item : selectedColumns) {
            if (item.getExpression() instanceof Column) {
                schema.add(((Column) item.getExpression()).getFullyQualifiedName());
            } else if (item.toString().contains("SUM")) {
                schema.add(item.toString());
            }
        }
        return schema;
    }
}