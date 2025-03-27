package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.*;

/**
 * The SumOperator class is responsible for performing the sum operation on the selected columns.
 * It takes a child operator as input and the selected columns to perform the sum operation.
 * <br><br>
 * The SumOperator class contains the following methods:<br>
 * - getNextTuple(): Retrieves the next tuple with the sum of the selected columns.<br>
 * - reset(): Resets the iterator to the start.<br>
 * - getTableName(): Returns the name of the table.<br>
 * - getTableSchema(): Returns the schema of the table.
 * <br><br>
 * The SumOperator class also contains the following instance variables:<br>
 * - childOperator: The child operator of the SumOperator.<br>
 * - schema: The schema of the table from the child operator.<br>
 * - selectedColumns: The selected columns to perform the sum operation.<br>
 * - groupByColumns: The group by columns for the sum operation.<br>
 * - sumExpIndexesList: A list of indexes of the selected columns that contain the sum operation.<br>
 * - groupColToTupleAndSumMapping: A mapping of group by columns to the tuple values and sum values.<br>
 * - groupIterator: An iterator to iterate over the group by columns.
 */

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

        // Find the indexes of the selected columns that contain the SUM operation
        int selectColumnsIndex = 0;
        for (SelectItem<?> item : selectedColumns) {
            if (item.toString().contains("SUM")) {
                sumExpIndexesList.add(selectColumnsIndex++);
            } else {
                selectColumnsIndex++;
            }
        }
    }

    /**
     * Retrieves the next tuple with the sum of the selected columns.
     * @return The next tuple with the sum of the selected columns.
     *
     * @Description
     * If the groupIterator is null, compute the group sums. This is a blocking operation.<br>
     * For loop: Pick the values of the columns from the SELECT clause and the SUM clause and combine them
     * into a single tuple.<br>
     * If the selected column is an AllColumns, pick all the group by columns because the * indicates a subset
     * of the group by columns.<br>
     * If the selected column is a Column, pick the column value based on the column name.<br>
     * If the selected column contains the SUM operation, pick the sum values, which are stored after
     * the tuple values.
     */
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
                Expression selectedColumn = selectedColumns.get(selectedColumnIndex).getExpression();
                if (selectedColumn instanceof AllColumns) {
                    for (Column groupByColumn : groupByColumns) {
                        String columnFullName = groupByColumn.getFullyQualifiedName();
                        int columnIndex = schema.indexOf(columnFullName);
                        pickAndCombinedTuple.add(entry.getValue().get(columnIndex));
                    }
                } else if (selectedColumn instanceof Column) {
                    String columnName = ((Column) selectedColumns.get(selectedColumnIndex).getExpression()).getFullyQualifiedName();
                    int columnIndex = schema.indexOf(columnName);
                    pickAndCombinedTuple.add(entry.getValue().get(columnIndex));
                } else if (selectedColumn.toString().contains("SUM")) {
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

    /**
     * Computes the group sums by iterating through the child operator and storing the sum values
     * in the groupColToTupleAndSumMapping.
     * If there is a group by clause, the group by columns are used as the key.
     * If there is no group by clause, an empty string is used as the key.
     */
    private void computeGroupSums() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {

            // Initialize the key to the mapping and put into the tuples
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

            // Iterate through the sum expressions, calculate the sum values and append them to the tuple values
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

    /**
     * Returns the schema of the table.
     * The first part of the schema is the same as the schema of the child operator.
     * If the selected column contains the SUM operation, as the summed values are appended to the tuple values,
     * the schema is extended to match the summed values.
     * @return A list of strings representing the schema of the table.
     */
    @Override
    public List<String> getTableSchema() {
        List<String> schema = new ArrayList<>(this.schema);
        for (SelectItem<?> item : selectedColumns) {
            if (item.toString().contains("SUM")) {
                schema.add(item.toString());
            }
        }
        return schema;
    }
}