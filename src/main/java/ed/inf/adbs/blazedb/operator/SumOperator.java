package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.*;

public class SumOperator extends Operator {
    private final Operator childOperator;
    private final List<String> schema;
    private final List<SelectItem<?>> selectedColumns;
    private final ExpressionList<Column> groupByColumns;
    private final List<Integer> sumExpIndexesList;
    private final Map<String, List<Integer>> groupColToTupleAndSumMapping;
    private Iterator<Map.Entry<String, List<Integer>>> groupIterator;

    public SumOperator(Operator childOperator, List<SelectItem<?>> selectedColumns, ExpressionList groupByColumns) {
        this.childOperator = childOperator;
        this.schema = childOperator.getTableSchema();
        this.selectedColumns = selectedColumns;
        this.groupByColumns = groupByColumns;
        this.sumExpIndexesList = new ArrayList<>();
        this.groupColToTupleAndSumMapping = new HashMap<>();
        this.groupIterator = null;

        int selectColumnsIndex = 0;
        for (SelectItem<?> item : selectedColumns) {
            if (item.toString().contains("SUM")) {
                sumExpIndexesList.add(selectColumnsIndex++);
            } else {
                selectColumnsIndex++;
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
            Map.Entry<String, List<Integer>> entry = groupIterator.next();
            List<Integer> pickAndCombinedTuple = new ArrayList<>();
            for (int selectedColumnIndex = 0; selectedColumnIndex < selectedColumns.size(); selectedColumnIndex++) {
                if (selectedColumns.get(selectedColumnIndex).getExpression() instanceof AllColumns) {
                    for (Column groupByColumn : groupByColumns) {
                        String columnFullName = groupByColumn.getFullyQualifiedName();
                        int columnIndex = schema.indexOf(columnFullName);
                        pickAndCombinedTuple.add(entry.getValue().get(columnIndex));
                    }
                } else if (selectedColumns.get(selectedColumnIndex).getExpression() instanceof Column) {
                    String columnName = ((Column) selectedColumns.get(selectedColumnIndex).getExpression()).getFullyQualifiedName();
                    int columnIndex = schema.indexOf(columnName);
                    pickAndCombinedTuple.add(entry.getValue().get(columnIndex));
                } else if (selectedColumns.get(selectedColumnIndex).toString().contains("SUM")) {
                    int childSchemaSize = schema.size();
                    int sumExpIndex = 0;
                    while (selectedColumnIndex < selectedColumns.size()) {
                        pickAndCombinedTuple.add(entry.getValue().get(childSchemaSize + sumExpIndex));
                        selectedColumnIndex++;
                        sumExpIndex++;
                    }
                }
            }
            return new Tuple(pickAndCombinedTuple);
        } else {
            return null;
        }
    }

    private void computeGroupSums() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {

            List<Integer> groupKeyList = new ArrayList<>();
            String groupKeyString = "";
            if (groupByColumns != null) {
                for (Column groupByColumn : groupByColumns) {
                    String columnFullName = groupByColumn.getFullyQualifiedName();
                    groupKeyList.add(tuple.getValue(schema.indexOf(columnFullName)));
                }
                groupKeyString = groupKeyList.toString();
                if (!groupColToTupleAndSumMapping.containsKey(groupKeyString)) {
                    groupColToTupleAndSumMapping.put(groupKeyString, new ArrayList<>(tuple.getValues()));
                }
            } else {
                if (groupColToTupleAndSumMapping.keySet().isEmpty()) {
                    groupColToTupleAndSumMapping.put(groupKeyString, new ArrayList<>(tuple.getValues()));
                }
            }

            int tupleLength = tuple.getValues().size();
            for (int sumExpIndex = 0; sumExpIndex < sumExpIndexesList.size(); sumExpIndex++) {
                int currentIndex = sumExpIndexesList.get(sumExpIndex);
                ExpressionEvaluator sumExpEvaluator = new ExpressionEvaluator(tuple, schema);
                selectedColumns.get(currentIndex).accept(sumExpEvaluator);

                int sumValue = sumExpEvaluator.getValue();

                List<Integer> existingValues = groupColToTupleAndSumMapping.get(groupKeyString);
                if (existingValues.size() <= tupleLength + sumExpIndex) {
                    existingValues.add(0);
                }
                existingValues.set(tupleLength + sumExpIndex, existingValues.get(tupleLength + sumExpIndex) + sumValue);
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
        List<String> schema = new ArrayList<>(this.schema);
        for (SelectItem<?> item : selectedColumns) {
//            if (item.getExpression() instanceof Column) {
//                schema.add(((Column) item.getExpression()).getFullyQualifiedName());
//            } else
            if (item.toString().contains("SUM")) {
                schema.add(item.toString());
            }
        }
        return schema;
    }
}