package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

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
        return buildOperatorTree();
    }

    private void extractConditions(Expression where) {
        if (where == null) return;
        // Recursively separate join and selection conditions
        ConditionExtractor.extract(where, this.select, selectionConditions, joinConditions);
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

    private Operator buildOperatorTree() throws IOException {
        Iterator<String> tableNames = getFromTables().iterator();
        Operator root = tableScans.get(tableNames.next());

        while (tableNames.hasNext()) {
            String nextTableName = tableNames.next();
            Operator right = tableScans.get(nextTableName);
            Expression joinCondition = findJoinCondition(root.getTableName(), nextTableName);
            root = new JoinOperator(root, right, joinCondition);
        }

        // TODO: solve the case where there is a combination of * and SUM
        if (!(select.getSelectItems().get(0).getExpression() instanceof AllColumns)) {
            if (select.getGroupBy() != null) {
                root = new ProjectOperator(root, select.getSelectItems(), select.getGroupBy().getGroupByExpressionList());
            } else {
                root = new ProjectOperator(root, select.getSelectItems(), null);
            }
        }

        if (select.getSelectItems().stream().anyMatch(item -> item.toString().contains("SUM")) || select.getGroupBy() != null) {
            if (select.getGroupBy() != null) {
                root = new SumOperator(root, select.getSelectItems(), select.getGroupBy().getGroupByExpressionList());
            } else {
                root = new SumOperator(root, select.getSelectItems(), null);
            }
        }

        if (select.getOrderByElements() != null) {
            root = new SortOperator(root, select.getOrderByElements());
        }

        if (select.getDistinct() != null) {
            root = new DuplicateEliminationOperator(root);
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
        if (conditions.isEmpty() && leftTable.contains("JOIN")) {
            String[] joinedTables = leftTable.split(" JOIN ");
            for (String table : joinedTables) {
                Expression condition = findJoinCondition(table, rightTable);
                if (condition != null) {
                    conditions.add(condition);
                }
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
        List<String> tableNames = new ArrayList<>();
        tableNames.add(select.getFromItem().toString());
        if (select.getJoins() != null) {
            select.getJoins().forEach(join -> tableNames.add(join.toString()));
        }
        return tableNames;
    }
}
