package com.lizurt.metaforge.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class PrimaryKeyFixerService {

    public void fixPrimaryKeys(String dbUrl, String dbUser, String dbPassword) {
        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            List<String> tables = getTables(connection);

            for (String table : tables) {
                List<String> columns = getColumns(connection, table);
                log.info("Trying to find a primary key for a table {}", table);
                List<String> primaryKey = findPrimaryKey(connection, table, columns);

                if (!primaryKey.isEmpty()) {
                    // the primary key was found, add the constraint then
                    createPrimaryKey(connection, table, primaryKey);
                } else {
                    log.warn("Failed to find a primary key for a table {}", table);
                }
            }
        } catch (SQLException e) {
            log.error("Something went wrong during fixing primary keys", e);
        }
    }

    private List<String> getTables(Connection connection) throws SQLException {
        List<String> tables = new ArrayList<>();
        String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';";
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                tables.add(rs.getString("table_name"));
            }
        }
        return tables;
    }

    private List<String> getColumns(Connection connection, String table) throws SQLException {
        List<String> columns = new ArrayList<>();
        String query = "SELECT column_name FROM information_schema.columns WHERE table_name = ?;";
        try (PreparedStatement pstmt = connection.prepareStatement(query)) {
            pstmt.setString(1, table);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString("column_name"));
                }
            }
        }
        return columns;
    }

    private List<String> findPrimaryKey(Connection connection, String table, List<String> columns) throws SQLException {
        int totalRows = getTotalRowCount(connection, table);

        // iterate all columns and their combinations
        for (int i = 1; i <= columns.size(); i++) {
            List<List<String>> combinations = generateCombinations(columns, i);

            for (List<String> combination : combinations) {
                String concatenatedColumns = combination.stream()
                        .map(column -> "COALESCE(" + column + "::TEXT, '')")
                        .collect(Collectors.joining(" || '-' || "));

                String query = String.format(
                        "SELECT COUNT(DISTINCT %s) FROM %s;",
                        concatenatedColumns, table
                );

                try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
                    if (rs.next()) {
                        int uniqueRows = rs.getInt(1);
                        if (uniqueRows == totalRows) {
                            // we got the combination
                            return combination;
                        }
                    }
                }
            }
        }

        // no combinations found
        return new ArrayList<>();
    }

    private int getTotalRowCount(Connection connection, String table) throws SQLException {
        String query = "SELECT COUNT(*) FROM " + table;
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        return 0;
    }

    private void createPrimaryKey(Connection connection, String table, List<String> primaryKey) throws SQLException {
        String primaryKeyColumns = String.join(", ", primaryKey);
        String primaryKeyName = "pk_" + table;

        String query = String.format(
                "ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s);",
                table, primaryKeyName, primaryKeyColumns
        );

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(query);
            log.info("Added a primary key ({}) for a table {}", primaryKeyColumns, table);
        } catch (SQLException e) {
            log.error("Something went wrong during adding a primary key ({}) for a table {}", primaryKeyColumns, table, e);
        }
    }

    private List<List<String>> generateCombinations(List<String> columns, int r) {
        List<List<String>> combinations = new ArrayList<>();
        generateCombinationsRecursive(columns, new ArrayList<>(), 0, r, combinations);
        return combinations;
    }

    private void generateCombinationsRecursive(List<String> columns, List<String> current, int index, int r, List<List<String>> combinations) {
        if (current.size() == r) {
            combinations.add(new ArrayList<>(current));
            return;
        }
        if (index >= columns.size()) {
            return;
        }

        // add element to the combination
        current.add(columns.get(index));
        generateCombinationsRecursive(columns, current, index + 1, r, combinations);

        // skip element
        current.removeLast();
        generateCombinationsRecursive(columns, current, index + 1, r, combinations);
    }
}
