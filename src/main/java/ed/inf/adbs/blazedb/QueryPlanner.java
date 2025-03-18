package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
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

        // todo: handle multiple joins
        while (tables.hasNext()) {
            String nextTable = tables.next();
            Operator right = tableScans.get(nextTable);
            Expression joinCondition = findJoinCondition(root.getTableName(), nextTable);
            root = new JoinOperator(root, right, joinCondition);
        }

//        if (!select.getSelectItems().get(0).toString().equals("*")) {
        if (!(select.getSelectItems().get(0).getExpression() instanceof AllColumns)) {
            root = new ProjectOperator(root, select.getSelectItems());
        }
        return root;
    }

private Expression findJoinCondition(String leftTable, String rightTable) {
    List<Expression> conditions = new ArrayList<>();
    for (Expression cond : joinConditions) {
        if (ConditionExtractor.isJoinCondition(cond, leftTable, rightTable)) {
            conditions.add(cond);
        }
    }
    if (conditions.isEmpty()) {
        return null;
    }
    Expression combinedCondition = conditions.get(0);
    for (int i = 1; i < conditions.size(); i++) {
        combinedCondition = new AndExpression(combinedCondition, conditions.get(i));
    }
    return combinedCondition;
}

    private List<String> getFromTables() {
        List<String> tables = new ArrayList<>();
        tables.add(select.getFromItem().toString());
        if (select.getJoins() != null) {
            select.getJoins().forEach(join -> tables.add(join.toString()));
        }
        return tables;
    }
}
