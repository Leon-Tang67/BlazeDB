package ed.inf.adbs.blazedb;

import java.util.HashMap;
import java.util.Map;

import java.io.*;
import java.util.*;

/**
 * DatabaseCatalog class is a singleton class that stores the schema of the database.
 * It reads the schema from the schema.txt file and stores it in a map.
 * It also stores the file path of the table in a map.
 * <br><br>
 * The DatabaseCatalog class contains the following methods:<br>
 * - getInstance(): Returns the instance of the DatabaseCatalog class.<br>
 * - loadSchema(): Loads the schema from the schema.txt file.<br>
 * - getTableFilePath(): Returns the file path of the table.<br>
 * - getTableSchema(): Returns the schema of the table.
 * <br><br>
 * The DatabaseCatalog class also contains the following instance variables:<br>
 * - instance: The instance of the DatabaseCatalog class.<br>
 * - tableFileMap: A map that stores the file path of the table.<br>
 * - tableSchemaMap: A map that stores the schema of the table.
 */

public class DatabaseCatalog {
    private static DatabaseCatalog instance;
    private final Map<String, String> tableFileMap; // Maps table names to file paths
    private final Map<String, List<String>> tableSchemaMap; // Maps table names to their schemas

    /**
     * Constructor for DatabaseCatalog class.
     * Reads the schema from the schema.txt file and stores it in the tableSchemaMap.
     * @param databaseDir The directory where the database files are stored.
     */
    private DatabaseCatalog(String databaseDir) throws IOException {
        tableFileMap = new HashMap<>();
        tableSchemaMap = new HashMap<>();
        loadSchema(databaseDir);
    }

    /**
     * Returns the instance of the DatabaseCatalog class.
     * @param databaseDir The directory where the database files are stored.
     * @return The instance of the DatabaseCatalog class.
     */
    public static DatabaseCatalog getInstance(String databaseDir) throws IOException {
        if (instance == null) {
            instance = new DatabaseCatalog(databaseDir);
        }
        return instance;
    }

    /**
     * Loads the schema from the schema.txt file.
     * @param databaseDir The directory where the database files are stored.
     */
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

    /**
     * Returns the file path of the table.
     * @param tableName The name of the table.
     * @return The file path of the table.
     */
    public String getTableFilePath(String tableName) {
        return tableFileMap.get(tableName);
    }

    /**
     * Returns the schema of the table.
     * @param tableName The name of the table.
     * @return The schema of the table.
     */
    public List<String> getTableSchema(String tableName) {
        return tableSchemaMap.get(tableName);
    }
}