package ed.inf.adbs.blazedb;

import java.util.HashMap;
import java.util.Map;

import java.io.*;
import java.util.*;

public class DatabaseCatalog {
    private static DatabaseCatalog instance;
    private Map<String, String> tableFileMap; // Maps table names to file paths
    private Map<String, List<String>> tableSchemaMap; // Maps table names to their schemas

    private DatabaseCatalog(String databaseDir) throws IOException {
        tableFileMap = new HashMap<>();
        tableSchemaMap = new HashMap<>();
        loadSchema(databaseDir);
    }

    public static DatabaseCatalog getInstance(String databaseDir) throws IOException {
        if (instance == null) {
            instance = new DatabaseCatalog(databaseDir);
        }
        return instance;
    }

    private void loadSchema(String databaseDir) throws IOException {
        File schemaFile = new File(databaseDir + "/schema.txt");
        try (BufferedReader br = new BufferedReader(new FileReader(schemaFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(" ");
                String tableName = parts[0];
                List<String> columns = new ArrayList<>();
                for (int i = 1; i < parts.length; i++) {
                    columns.add(tableName + "." + parts[i]);
                }
                tableSchemaMap.put(tableName, columns);
                tableFileMap.put(tableName, databaseDir + "/data/" + tableName + ".csv");
            }
        }
    }

    public String getTableFilePath(String tableName) {
        return tableFileMap.get(tableName);
    }

    public List<String> getTableSchema(String tableName) {
        return tableSchemaMap.get(tableName);
    }
}