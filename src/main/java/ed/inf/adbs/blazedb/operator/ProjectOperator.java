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

    public ProjectOperator(Operator childOperator, List<SelectItem<?>> selectedColumns) throws IOException {
        this.childOperator = childOperator;
        List<String> schema = DatabaseCatalog.getInstance("").getTableSchema(this.getTableName());

        // Convert column names to indexes
        columnIndexes = new ArrayList<>();
        for (SelectItem item : selectedColumns) {
            columnIndexes.add(schema.indexOf(((Column) item.getExpression()).getColumnName()));
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
        // Traverse down the tree until we reach the ScanOperator
        while (childOperator instanceof SelectOperator) {
            childOperator = ((SelectOperator) childOperator).getChild();
        }
        // Now, childOperator should be a ScanOperator, which has tableName
        if (childOperator instanceof ScanOperator) {
            return ((ScanOperator) childOperator).getTableName();
        }
        throw new IllegalArgumentException("Invalid operator tree structure: No ScanOperator found.");
    }

}

