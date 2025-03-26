package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;

import java.util.List;

/**
 * The abstract Operator class for the iterator model.
 * The Operator class is the base class for all operators in the query execution plan.
 *
 * The Operator class contains the following methods:
 * - getNextTuple(): Retrieves the next tuple from the iterator.
 * - reset(): Resets the iterator to the start.
 * - getTableName(): Returns the name of the table.
 * - getTableSchema(): Returns the schema of the table.
 */
public abstract class Operator {

    /**
     * Retrieves the next tuple from the iterator.
     * @return A Tuple object representing the row of data, or NULL if EOF reached.
     */
    public abstract Tuple getNextTuple();

    /**
     * Resets the iterator to the start.
     */
    public abstract void reset();

    /**
     * Returns the name of the table.
     * @return The name of the table.
     */
    public abstract String getTableName();

    /**
     * Returns the schema of the table.
     * @return A list of column names representing the schema of the table.
     */
    public abstract List<String> getTableSchema();
}