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

public class ProjectOperator extends Operator {
    private final Operator childOperator;
    private final List<Integer> columnIndexes;
    private final List<String> schema;

    public ProjectOperator(Operator childOperator, PlainSelect select) throws IOException {
        this.childOperator = childOperator;
        this.schema = childOperator.getTableSchema();
        List<SelectItem<?>> selectedColumns = select.getSelectItems();
        ExpressionList groupByExpressionLists = null;
        if (select.getGroupBy() != null) {
            groupByExpressionLists = select.getGroupBy().getGroupByExpressionList();
        }
        List<OrderByElement> orderByElements = select.getOrderByElements();

        // Convert column names to indexes
        Set<String> columnNameSet = new HashSet<>();
        columnIndexes = new ArrayList<>();

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

        for (SelectItem<?> item : selectedColumns) {
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
            } else
            if (item.toString().contains("SUM")) {
                if (!(item.getExpression() instanceof Function)) {
                    break;
                }

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
                        // TODO: add comments
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
                } else if (function.getParameters().get(0) instanceof LongValue) {
                    if (groupByExpressionLists != null) {
                        for (Object column : groupByExpressionLists) {
                            Column groupByColumn = (Column) column;
                            String columnFullName = groupByColumn.getFullyQualifiedName();
                            if (!columnNameSet.contains(columnFullName)) {
                                columnNameSet.add(columnFullName);
                                columnIndexes.add(schema.indexOf(columnFullName));
                            }
                        }
                    } else {
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

    @Override
    public List<String> getTableSchema() {
        List<String> projectedSchema = new ArrayList<>();
        for (int index : columnIndexes) {
            projectedSchema.add(schema.get(index));
        }
        return projectedSchema;
    }
}
