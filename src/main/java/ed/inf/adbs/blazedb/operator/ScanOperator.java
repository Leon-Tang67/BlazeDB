package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;
import java.io.*;
import java.util.*;

public class ScanOperator extends Operator {
    private BufferedReader reader;
    private String tableName;
    private String filePath;

    public ScanOperator(String tableName) throws IOException {
        this.tableName = tableName;
        this.filePath = DatabaseCatalog.getInstance("").getTableFilePath(tableName);
        reset();
    }

    @Override
    public Tuple getNextTuple() {
        try {
            String line = reader.readLine();
            if (line == null) return null;
            Integer[] values = Arrays.stream(line.split(",")).map(String::trim).map(Integer::parseInt).toArray(Integer[]::new);
            return new Tuple(Arrays.asList(values));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void reset() {
        try {
            if (reader != null) {
                reader.close();
            }
            reader = new BufferedReader(new FileReader(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getTableName() {
        return tableName;
    }
}
