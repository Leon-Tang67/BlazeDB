package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.IOException;
import java.util.*;

public class QueryPlanner {
    private PlainSelect select;
    private Map<String, Operator> tableScans;
    private List<Expression> joinConditions;
    private Map<String, Expression> selectionConditions;

    public QueryPlanner(PlainSelect select) {
        this.select = select;
        this.tableScans = new HashMap<>();
        this.joinConditions = new ArrayList<>();
        this.selectionConditions = new HashMap<>();
    }

    public Operator generatePlan() throws IOException {
        extractConditions(select.getWhere());
        buildTableScans();
        return buildJoinTree();
    }

    private void extractConditions(Expression where) {
        if (where == null) return;
        // Recursively separate join and selection conditions
        ConditionExtractor.extract(where, selectionConditions, joinConditions);
    }

    private void buildTableScans() throws IOException {
        for (String table : getFromTables()) {
            Operator scan = new ScanOperator(table);
            if (selectionConditions.containsKey(table)) {
                scan = new SelectOperator(scan, selectionConditions.get(table));
            }
            tableScans.put(table, scan);
        }
    }

    private Operator buildJoinTree() throws IOException {
        Iterator<String> tables = getFromTables().iterator();
        Operator root = tableScans.get(tables.next());

        while (tables.hasNext()) {
            String nextTable = tables.next();
            Operator right = tableScans.get(nextTable);
            Expression joinCondition = findJoinCondition(root.getTableName(), nextTable);
            root = new JoinOperator(root, right, joinCondition);
        }

        if (!select.getSelectItems().get(0).toString().equals("*")) {
            root = new ProjectOperator(root, select.getSelectItems());
        }
        return root;
    }

    private Expression findJoinCondition(String leftTable, String rightTable) {
        for (Expression cond : joinConditions) {
            if (ConditionExtractor.isJoinCondition(cond, leftTable, rightTable)) {
                return cond;
            }
        }
        return null;
    }

    private List<String> getFromTables() {
        return select.getFromItem().toString().contains(" ")
                ? Arrays.asList(select.getFromItem().toString().split(","))
                : Collections.singletonList(select.getFromItem().toString());
    }
}
