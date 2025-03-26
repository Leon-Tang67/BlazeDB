package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * The ScanOperator class is responsible for scanning the table and returning the tuples.
 * It reads the table file and returns the tuples one by one.
 *
 * The ScanOperator class extends the Operator class and implements the following methods:
 * - getNextTuple(): Retrieves the next tuple from the table.
 * - reset(): Resets the iterator to the start of the table.
 * - getTableName(): Returns the name of the table.
 * - getTableSchema(): Returns the schema of the table.
 *
 * The ScanOperator class also contains the following instance variables:
 * - reader: The BufferedReader object used to read the table file.
 * - tableName: The name of the table being scanned.
 * - tableFilePath: The file path of the table being scanned.
 * - tableSchema: The schema of the table being scanned.
 */

public class ScanOperator extends Operator {
    private BufferedReader reader;
    private final String tableName;
    private final String tableFilePath;
    private final List<String> tableSchema;

    public ScanOperator(String tableName) throws IOException {
        this.tableName = tableName;
        this.tableFilePath = DatabaseCatalog.getInstance("").getTableFilePath(tableName);
        this.tableSchema = DatabaseCatalog.getInstance("").getTableSchema(tableName);
        this.reader = new BufferedReader(new FileReader(tableFilePath));
    }

    @Override
    public Tuple getNextTuple() {
        try {
            String line = reader.readLine();
            if (line == null) return null;  // EOF or empty file

            String[] values = line.split(",");
            List<Integer> tupleValues = new ArrayList<>();
            for (String value : values) {
                tupleValues.add(Integer.parseInt(value.trim()));
            }
            return new Tuple(tupleValues);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void reset() {
        try {
            reader.close();
            reader = new BufferedReader(new FileReader(tableFilePath)); // Reopen the file
        } catch (IOException e) {
            throw new RuntimeException("Error resetting ScanOperator for table: " + tableName, e);
        }
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    public List<String> getTableSchema() {
        return tableSchema;
    }
}
