package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.IOException;

public class QueryPlanner {
    public Operator createQueryPlan(Statement statement) throws Exception {
        if (!(statement instanceof Select)) {
            throw new IllegalArgumentException("Only SELECT statements are supported.");
        }

        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement;

        // Step 1: Create ScanOperator for base table
        String tableName = plainSelect.getFromItem().toString();

        return getOperator(tableName, plainSelect);
    }

    private static Operator getOperator(String tableName, PlainSelect plainSelect) throws IOException {
        Operator operator = new ScanOperator(tableName);

        // Step 2: Wrap in SelectOperator if there's a WHERE clause
        if (plainSelect.getWhere() != null) {
            operator = new SelectOperator(operator, plainSelect.getWhere());
        }

        // Step 3: Wrap in ProjectionOperator if specific columns are selected
        if (plainSelect.getSelectItems() != null && !(plainSelect.getSelectItems().get(0).getExpression() instanceof AllColumns)) {
            operator = new ProjectOperator(operator, plainSelect.getSelectItems());
        }
        return operator;
    }
}
