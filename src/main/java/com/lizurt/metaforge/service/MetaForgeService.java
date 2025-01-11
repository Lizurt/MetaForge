package com.lizurt.metaforge.service;

import org.springframework.stereotype.Service;

import java.sql.*;

@Service
public class MetaForgeService {

    public void ruinDatabase(String dbUrl, String dbUser, String dbPassword) throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {

            // get all tables
            try (Statement tableStmt = connection.createStatement();
                 ResultSet tables = tableStmt.executeQuery(
                         "SELECT table_name " +
                                 "FROM information_schema.tables " +
                                 "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")) {

                while (tables.next()) {
                    String tableName = tables.getString("table_name");

                    // remove foreign keys
                    try (Statement fkStmt = connection.createStatement();
                         ResultSet fkConstraints = fkStmt.executeQuery(
                                 "SELECT conname " +
                                         "FROM pg_constraint " +
                                         "WHERE conrelid = '" + tableName + "'::regclass AND contype = 'f'")) {
                        while (fkConstraints.next()) {
                            String constraintName = fkConstraints.getString("conname");
                            try (Statement dropStmt = connection.createStatement()) {
                                dropStmt.execute("ALTER TABLE " + tableName + " DROP CONSTRAINT " + constraintName + " CASCADE");
                            }
                        }
                    }

                    // remove unique and primary keys
                    try (Statement pkStmt = connection.createStatement();
                         ResultSet pkConstraints = pkStmt.executeQuery(
                                 "SELECT conname " +
                                         "FROM pg_constraint " +
                                         "WHERE conrelid = '" + tableName + "'::regclass AND contype IN ('u', 'p')")) {
                        while (pkConstraints.next()) {
                            String constraintName = pkConstraints.getString("conname");
                            try (Statement dropStmt = connection.createStatement()) {
                                dropStmt.execute("ALTER TABLE " + tableName + " DROP CONSTRAINT " + constraintName + " CASCADE");
                            }
                        }
                    }

                    // remove indices
                    try (Statement indexStmt = connection.createStatement();
                         ResultSet indexes = indexStmt.executeQuery(
                                 "SELECT indexname " +
                                         "FROM pg_indexes " +
                                         "WHERE tablename = '" + tableName + "'")) {
                        while (indexes.next()) {
                            String indexName = indexes.getString("indexname");
                            try (Statement dropStmt = connection.createStatement()) {
                                dropStmt.execute("DROP INDEX IF EXISTS " + indexName);
                            }
                        }
                    }

                    // remove not null
                    try (Statement columnStmt = connection.createStatement();
                         ResultSet columns = columnStmt.executeQuery(
                                 "SELECT column_name " +
                                         "FROM information_schema.columns " +
                                         "WHERE table_name = '" + tableName + "' AND is_nullable = 'NO'")) {
                        while (columns.next()) {
                            String columnName = columns.getString("column_name");
                            try (Statement alterStmt = connection.createStatement()) {
                                alterStmt.execute("ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " DROP NOT NULL");
                            }
                        }
                    }
                }
            }
        }
    }

    public void fixDatabase(String dbUrl, String dbUser, String dbPassword) {

    }
}
