package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProjectOperator extends Operator {
    private Operator childOperator;
    private List<Integer> columnIndexes;
    private List<String> schema;

    public ProjectOperator(Operator childOperator, List<SelectItem<?>> selectedColumns) throws IOException {
        this.childOperator = childOperator;
        this.schema = childOperator.getTableSchema();

        // Convert column names to indexes
        columnIndexes = new ArrayList<>();
        for (SelectItem<?> item : selectedColumns) {
            Column column = ((Column) item.getExpression());
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName();
            String columnFullName = tableName + "." + columnName;
            columnIndexes.add(schema.indexOf(columnFullName));
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
        List<String> schema = childOperator.getTableSchema();
        List<String> projectedSchema = new ArrayList<>();
        for (int index : columnIndexes) {
            projectedSchema.add(schema.get(index));
        }
        return projectedSchema;
    }
}
