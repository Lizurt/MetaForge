package com.lizurt.metaforge;

import com.lizurt.metaforge.service.MetaForgeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class PostgreSQLTests {

    @Value("${spring.datasource.url}")
    private String dbUrl;

    @Value("${spring.datasource.username}")
    private String dbUsername;

    @Value("${spring.datasource.password}")
    private String dbPassword;

    @Value("${metaforge.test.database.users.amount}")
    private int usersAmount;

    @Value("${metaforge.test.database.libraries.amount}")
    private int librariesAmount;

    @Value("${metaforge.test.database.visits.amount}")
    private int visitsAmount;

    @Value("${metaforge.test.random.seed}")
    private int randomSeed;

    private final MetaForgeService metaForgeService;

    @Test
    public void populateDatabaseAndTestConstraints() throws Exception {
        Random random = new Random(randomSeed);

        try (Connection connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS visits");
                statement.execute("DROP TABLE IF EXISTS libraries");
                statement.execute("DROP TABLE IF EXISTS users");

                statement.execute("CREATE TABLE users (" +
                        "user_id SERIAL PRIMARY KEY, " +
                        "age INT, " +
                        "salary FLOAT, " +
                        "name TEXT NOT NULL, " +
                        "surname TEXT NOT NULL, " +
                        "patronymic TEXT, " +
                        "login TEXT UNIQUE " +
                        ")");

                statement.execute("CREATE TABLE libraries (" +
                        "library_id SERIAL PRIMARY KEY, " +
                        "location TEXT NOT NULL " +
                        ")");

                statement.execute("CREATE TABLE visits (" +
                        "visit_id SERIAL PRIMARY KEY, " +
                        "user_id INT, " +
                        "library_id INT, " +
                        "visit_date TIMESTAMP, " +
                        "UNIQUE (user_id, library_id, visit_date), " +
                        "FOREIGN KEY (user_id) REFERENCES users(user_id), " +
                        "FOREIGN KEY (library_id) REFERENCES libraries(library_id) " +
                        ")");
            }

            try (PreparedStatement usersStatement =
                         connection.prepareStatement(
                                 "INSERT INTO users (age, salary, name, surname, patronymic, login) " +
                                         "VALUES (?, ?, ?, ?, ?, ?)")) {
                for (int i = 0; i < usersAmount; i++) {
                    usersStatement.setInt(1, random.nextInt(100));
                    usersStatement.setFloat(2, random.nextFloat() * 100000);
                    usersStatement.setString(3, "John" + i);
                    usersStatement.setString(4, "Doe" + i);
                    usersStatement.setString(5, "Patronymic" + i);
                    usersStatement.setString(6, "johndoe" + i);
                    usersStatement.addBatch();
                }
                usersStatement.executeBatch();
            }

            try (PreparedStatement librariesStatement =
                         connection.prepareStatement(
                                 "INSERT INTO libraries (location) VALUES (?)")) {
                for (int i = 0; i < librariesAmount; i++) {
                    librariesStatement.setString(1, "Revolutsii street, " + i);
                    librariesStatement.addBatch();
                }
                librariesStatement.executeBatch();
            }

            try (PreparedStatement visitsStatement =
                         connection.prepareStatement(
                                 "INSERT INTO visits (user_id, library_id, visit_date) VALUES (?, ?, ?)")) {
                for (int i = 0; i < visitsAmount; i++) {
                    visitsStatement.setInt(1, random.nextInt(usersAmount) + 1);
                    visitsStatement.setInt(2, random.nextInt(librariesAmount) + 1);
                    visitsStatement.setTimestamp(3, Timestamp.from(Instant.now().plus(i, ChronoUnit.DAYS)));
                    visitsStatement.addBatch();
                }
                visitsStatement.executeBatch();
            }

            metaForgeService.ruinDatabase(dbUrl, dbUsername, dbPassword);
            metaForgeService.fixDatabase(dbUrl, dbUsername, dbPassword);

            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery(
                        """
                                SELECT column_name, constraint_type \
                                FROM information_schema.table_constraints tc \
                                JOIN information_schema.constraint_column_usage ccu \
                                ON tc.constraint_name = ccu.constraint_name \
                                WHERE tc.table_name = 'users'
                                """
                )) {
                    while (rs.next()) {
                        String columnName = rs.getString("column_name");
                        String constraintType = rs.getString("constraint_type");
                        switch (columnName) {
                            case "user_id":
                                assertEquals("PRIMARY KEY", constraintType, "users table lost its PRIMARY KEY");
                                break;
                            case "name", "surname":
                                assertEquals("NOT NULL", constraintType, "users table lost its NOT NULL name and surname");
                                break;
                            case "login":
                                assertEquals("UNIQUE", constraintType, "users table lost its ALTERNATIVE KEY (UNIQUE login)");
                                break;
                        }
                    }
                }

                try (ResultSet rs = statement.executeQuery(
                        """
                                SELECT column_name, constraint_type \
                                FROM information_schema.table_constraints tc \
                                JOIN information_schema.constraint_column_usage ccu \
                                ON tc.constraint_name = ccu.constraint_name \
                                WHERE tc.table_name = 'visits'
                                """
                )) {
                    while (rs.next()) {
                        String columnName = rs.getString("column_name");
                        String constraintType = rs.getString("constraint_type");
                        switch (columnName) {
                            case "user_id, library_id":
                                assertEquals("FOREIGN KEY", constraintType, "visits table lost its FOREIGN KEYs");
                                break;
                            case "visit_id":
                                assertEquals("PRIMARY KEY", constraintType, "visits table lost its PRIMARY KEY");
                                break;
                        }
                    }
                }

                try (ResultSet resultSet = statement.executeQuery(
                        """
                                SELECT conname AS constraint_name \
                                FROM pg_constraint \
                                WHERE conrelid = 'visits'::regclass \
                                  AND contype = 'u' \
                                  AND conkey @> ARRAY[ \
                                      (SELECT attnum FROM pg_attribute WHERE attrelid = 'visits'::regclass AND attname = 'user_id'), \
                                      (SELECT attnum FROM pg_attribute WHERE attrelid = 'visits'::regclass AND attname = 'library_id'), \
                                      (SELECT attnum FROM pg_attribute WHERE attrelid = 'visits'::regclass AND attname = 'visit_date') \
                                  ];
                                """
                )) {
                    assertTrue(resultSet.next(), "visits table lost its alternative key (UNIQUE of user_id, library_id and date)");
                }


                try (ResultSet rs = statement.executeQuery(
                        """
                                SELECT column_name, constraint_type \
                                FROM information_schema.table_constraints tc \
                                JOIN information_schema.constraint_column_usage ccu \
                                ON tc.constraint_name = ccu.constraint_name \
                                WHERE tc.table_name = 'libraries'
                                """
                )) {
                    while (rs.next()) {
                        String columnName = rs.getString("column_name");
                        String constraintType = rs.getString("constraint_type");
                        switch (columnName) {
                            case "library_id":
                                assertEquals("PRIMARY KEY", constraintType, "libraries table lost its PRIMARY KEY");
                                break;
                            case "location":
                                assertEquals("NOT NULL", constraintType, "libraries table lost its NOT NULL location");
                                break;
                        }
                    }
                }
            }
        }
    }
}
