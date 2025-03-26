package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The ProjectOperator class is responsible for projecting the columns specified in the SELECT clause.
 * It takes a child operator as input and projects the specified columns from the child operator's output.
 *
 * The ProjectOperator class contains the following methods:
 * - getNextTuple(): Retrieves the next tuple with the projected columns.
 * - reset(): Resets the iterator to the start.
 * - getTableName(): Returns the name of the table.
 * - getTableSchema(): Returns the schema of the table.
 *
 * The ProjectOperator class also contains the following instance variables:
 * - childOperator: The child operator of the ProjectOperator.
 * - columnIndexes: A list of column indexes to be projected.
 * - schema: The schema of the table.
 */

public class ProjectOperator extends Operator {
    private final Operator childOperator;
    private final List<Integer> columnIndexes;
    private final List<String> schema;

    /**
     * Initializes the ProjectOperator with the child operator and the SELECT clause.
     * @param childOperator The child operator of the ProjectOperator.
     * @param select The SELECT clause containing the columns to be projected.
     * @throws IOException If an I/O error occurs.
     */
    public ProjectOperator(Operator childOperator, PlainSelect select) throws IOException {
        this.childOperator = childOperator;
        this.schema = childOperator.getTableSchema();

        // Extract the columns to be projected, group by columns, and order by columns for later use
        List<SelectItem<?>> selectedColumns = select.getSelectItems();
        ExpressionList groupByExpressionLists = null;
        if (select.getGroupBy() != null) {
            groupByExpressionLists = select.getGroupBy().getGroupByExpressionList();
        }
        List<OrderByElement> orderByElements = select.getOrderByElements();

        // Convert column names to indexes
        Set<String> columnNameSet = new HashSet<>();
        columnIndexes = new ArrayList<>();

        // Add group by columns to the column indexes if there are any
        if (groupByExpressionLists != null) {
            for (Object column : groupByExpressionLists) {
                Column groupByColumn = (Column) column;
                String columnFullName = groupByColumn.getFullyQualifiedName();
                if (!columnNameSet.contains(columnFullName)) {
                    columnNameSet.add(columnFullName);
                    columnIndexes.add(schema.indexOf(columnFullName));
                }
            }
        }

        // Add order by columns to the column indexes if there are any
        if (orderByElements != null) {
            for (OrderByElement column : orderByElements) {
                Column orderByColumn = (Column) column.getExpression();
                String columnFullName = orderByColumn.getFullyQualifiedName();
                if (!columnNameSet.contains(columnFullName)) {
                    columnNameSet.add(columnFullName);
                    columnIndexes.add(schema.indexOf(columnFullName));
                }
            }
        }

        // Scan through the selected columns and add them to the column indexes based on different scenarios
        for (SelectItem<?> item : selectedColumns) {
            // If the SELECT clause contains *, add all columns to the column indexes
            //     One special case is there is a GROUP BY clause, then we don't need to add all the columns
            //     as we know the columns indicated by * must be a subset of the group by columns
            // If the select item is a column, add it to the column indexes. This is the normal projection case
            //     even if there is a GROUP BY clause we still need to project these columns
            // If the select item is a SUM function, add the column(s) contained in the function to the column
            //     indexes for the SUM operator to use later in the calculation.
            if (item.getExpression() instanceof AllColumns) {
                if (groupByExpressionLists != null) {
                    continue;
                }
                columnNameSet.addAll(schema);
                columnIndexes.addAll(IntStream.range(0, schema.size()).boxed().collect(Collectors.toList()));
            } else if (item.getExpression() instanceof Column) {
                Column column = (Column) item.getExpression();
                String columnFullName = column.getFullyQualifiedName();
                if (!columnNameSet.contains(columnFullName)) {
                    columnNameSet.add(columnFullName);
                    columnIndexes.add(schema.indexOf(columnFullName));
                }
            } else if (item.toString().contains("SUM")) {
                if (!(item.getExpression() instanceof Function)) {
                    break;
                }

                // Recursively extract the column(s) contained in the SUM function to handle the case where
                // there are multiple columns multiplied together
                Function function = (Function) item.getExpression();
                if (function.getParameters().get(0) instanceof Multiplication) {
                    Multiplication multiplication = (Multiplication) function.getParameters().get(0);
                    while (true) {
                        if (multiplication.getRightExpression() instanceof Column) {
                            Column column = (Column) multiplication.getRightExpression();
                            String columnFullName = column.getFullyQualifiedName();
                            if (!columnNameSet.contains(columnFullName)) {
                                columnNameSet.add(columnFullName);
                                columnIndexes.add(schema.indexOf(columnFullName));
                            }
                        }
                        if (multiplication.getLeftExpression() instanceof Column) {
                            Column column = (Column) multiplication.getLeftExpression();
                            String columnFullName = column.getFullyQualifiedName();
                            if (!columnNameSet.contains(columnFullName)) {
                                columnNameSet.add(columnFullName);
                                columnIndexes.add(schema.indexOf(columnFullName));
                            }
                            break;
                        } else if (multiplication.getLeftExpression() instanceof LongValue) {
                            break;
                        } else if (multiplication.getLeftExpression() instanceof Multiplication) {
                            multiplication = (Multiplication) multiplication.getLeftExpression();
                        }
                    }
                } else if (function.getParameters().get(0) instanceof Column) {
                    Column column = (Column) function.getParameters().get(0);
                    String columnFullName = column.getFullyQualifiedName();
                    if (!columnNameSet.contains(columnFullName)) {
                        columnNameSet.add(columnFullName);
                        columnIndexes.add(schema.indexOf(columnFullName));
                    }
                // If the SUM function is applied to a constant value, add only the first column to the column indexes
                // for the calculation of line count. This can dramatically save tuple size to be stored in memory in
                // this special case
                } else if (function.getParameters().get(0) instanceof LongValue) {
                    if (groupByExpressionLists == null) {
                        columnIndexes.add(0);
                    }
                }
            }
        }
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple = childOperator.getNextTuple();
        if (tuple == null) return null;

        // Extract only required columns
        List<Integer> projectedValues = new ArrayList<>();
        for (int index : columnIndexes) {
            projectedValues.add(tuple.getValue(index));
        }
        return new Tuple(projectedValues);
    }

    @Override
    public void reset() {
        childOperator.reset();
    }

    @Override
    public String getTableName() {
        return childOperator.getTableName();
    }

    /**
     * Returns the schema of the table after projection.
     * The schema has changed to only include the columns that are projected.
     * @return The schema of the table.
     */
    @Override
    public List<String> getTableSchema() {
        List<String> projectedSchema = new ArrayList<>();
        for (int index : columnIndexes) {
            projectedSchema.add(schema.get(index));
        }
        return projectedSchema;
    }
}
