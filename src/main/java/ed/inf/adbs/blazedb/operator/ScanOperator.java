package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ScanOperator extends Operator {
    private BufferedReader reader;
    private String tableName;
    private String tableFilePath;
    private List<String> tableSchema;

    public ScanOperator(String tableName) throws IOException {
        this.tableName = tableName;
        this.tableFilePath = DatabaseCatalog.getInstance("").getTableFilePath(tableName);
        this.tableSchema = DatabaseCatalog.getInstance("").getTableSchema(tableName);
        this.reader = new BufferedReader(new FileReader(tableFilePath));
    }

    @Override
    public Tuple getNextTuple() {
        try {
            // TODO: Handle empty tables
            String line = reader.readLine();
            if (line == null) return null;

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
            reader = new BufferedReader(new FileReader(tableFilePath));
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
