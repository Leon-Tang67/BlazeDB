# BlazeDB
A lightweight database management system that supports SQL queries including 
SELECT, FROM, WHERE, ORDER BY, DISTINCT, GROUP BY and SUM.

## The logic for extracting join conditions from the WHERE clause
The logic for extracting join conditions from the WHERE clause is implemented in the 
`extract`, `categorizeCondition`, and `isJoinCondition` methods of the `ConditionExtractor` class.
The join conditions are extracted during building the operator tree in the `buildOperatorTree` method of the 
`QueryPlanner` class.
The logic is as follows:
1. The query is read and parsed in the `Interpreter` class, where the `QueryPlanner` instance is created with
the parsed plainSelect object as the argument.
2. Then by calling the `generatePlan` method of the `QueryPlanner` class, the operator tree will be built.
3. Within the `generatePlan` method, the first step is to extract the conditions, including join conditions and
selection conditions from the WHERE clause by calling the `extractConditions` method.
4. The `extractConditions` method calls the `extract` method of the `ConditionExtractor` class to extract the conditions.
5. The `extract` method recursively extracts conditions from the AndExpression until it becomes a BinaryExpression, 
and then calls `categorizeCondition` method to categorize the condition as a selection or join condition.
6. The `categorizeCondition` method checks if the condition is a join condition by checking the left and right table 
names on the two sides of the BinaryExpression. If the table names are different and doesn't contain `CONSTANT` keyword,
it is considered a join condition and put into the join condition list.
7. The `getTableName` method is used to get the table name from the expression. It returns the table name if the
expression is a Column object, otherwise returns the `CONSTANT` keyword.
8. Moving on to the `buildOperatorTree` method of the `QueryPlanner` class, while iterating through the tables in the 
FROM clause, `findJoinCondition` method is called to find the join conditions for the current tables.
9. Within the `findJoinCondition` method, it iterates through the join conditions list and checks if the left and right
table matches any join condition with the help of the `isJoinCondition` method.
10. For the situation where the left table is a joined table instead of a single table, its table name will contain
the `JOIN` keyword. This is realized by the `getTableName` method of the `JoinOperator` class.
11. In this case, the table names will be split and extracted from the joined table name and find the corresponding
join conditions.
12. If there are multiple join conditions, they are combined into an `AndExpression` for future use. 
13. The combined join conditions are then used to create the JoinOperator and add it to the operator tree.

## The optimization techniques used
1. **Selection Pushdown**: The selection pushdown optimization technique is used to push the selection conditions
down the operator tree to reduce the number of tuples that need to be processed. This is implemented in the
`QueryPlanner` class, after extracting conditions, which includes selection conditions, and during building table scans
with the `buildTableScans` method. After building the ScanOperator for each table, the corresponding selection 
conditions are extracted from the `selectionConditions` HashMap and add a selection operator with the selection 
conditions on the top of the scan operator.
2. **Projection Pushdown**: The projection pushdown optimization technique is used to push the projection operation
down the operator tree to reduce the number of columns that need to be processed. This is implemented in the
`QueryPlanner` class and `ProjectionOperator` class. During building the operator tree with the `buildOperatorTree` 
method by adding a projection operator on top of the scan operator or selection operator and pick only the essential 
columns for later use. The projection operator will only output the columns specified in the SELECT clause including
the columns included in the SUM clause, which will be used later in the SumOperator for calculation. One special case 
here is that when the SELECT clause starts with an AllColumn instance (*) and follows with SUM functions. In this case 
the projection operator will take in the GROUP BY columns as the output representatives for AllColumns. This is possible 
because the AllColumns instance in this case has to be a subset of the GROUP BY columns.