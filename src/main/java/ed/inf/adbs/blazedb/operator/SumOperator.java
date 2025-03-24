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
    private List<Integer> sumColumnIndexes;
    private ExpressionList groupByColumns;
    private Map<List<Integer>, Integer> groupSums;
    private Iterator<Map.Entry<List<Integer>, Integer>> groupIterator;

    public SumOperator(Operator childOperator, List<SelectItem<?>> selectedColumns, ExpressionList groupByColumns) {
        this.childOperator = childOperator;
        this.schema = childOperator.getTableSchema();
        this.selectedColumns = selectedColumns;
        this.sumColumnIndexes = new ArrayList<>();
        this.groupByColumns = groupByColumns;
        this.groupSums = new HashMap<>();
        this.groupIterator = null;

        int i = 0;
        for (SelectItem<?> item : selectedColumns) {
            if (item.getExpression() instanceof Column) {
                i++;
            } else if (item.toString().contains("SUM")) {
                sumColumnIndexes.add(i++);
            }
        }
    }

    @Override
    public Tuple getNextTuple() {
        if (groupIterator == null) {
            computeGroupSums();
            groupIterator = groupSums.entrySet().iterator();
        }

        if (groupIterator.hasNext()) {
            Map.Entry<List<Integer>, Integer> entry = groupIterator.next();
            List<Integer> values = new ArrayList<>(entry.getKey());
            values.add(entry.getValue());
            return new Tuple(values);
        } else {
            return null;
        }
    }

    private void computeGroupSums() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            List<Integer> groupKey = new ArrayList<>();
            if (groupByColumns == null) {
                for (int i = 0; i < sumColumnIndexes.size(); i++) {
                    groupKey.add(i);
                }
            } else {
                for (int i = 0; i < groupByColumns.size(); i++) {
                    Column column = (Column) groupByColumns.get(i);
                    String columnFullName = column.getFullyQualifiedName();
                    groupKey.add(tuple.getValue(schema.indexOf(columnFullName)));
                }
            }
            for (int i = 0; i < sumColumnIndexes.size(); i++) {
                int currentIndex = sumColumnIndexes.get(i);
                ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple, schema);
                selectedColumns.get(currentIndex).accept(evaluator);

                int value = evaluator.getValue();
                groupSums.put(groupKey, groupSums.getOrDefault(groupKey, 0) + value);
            }
        }
    }

    @Override
    public void reset() {
        childOperator.reset();
        groupSums.clear();
        groupIterator = null;
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
