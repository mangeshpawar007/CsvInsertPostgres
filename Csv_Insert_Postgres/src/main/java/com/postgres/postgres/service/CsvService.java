package com.postgres.postgres.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@Service
public class CsvService {

    @Value("${spring.datasource.url}")
    private String jdbcUrl;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;

    @Value("${filePath}")
    private String filePath;

    @Value("${tableName}")
    private String tableName;

    public void ingestData() {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            List<CSVRecord> records = readCSVFromResource();
            insertRecordsIntoPostgres(connection, tableName + RandomStringUtils.randomNumeric(3), records);
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    public List<CSVRecord> readCSVFromResource() throws IOException {
        try (InputStream inputStream = CsvService.class.getClassLoader().getResourceAsStream(filePath);
             CSVParser csvParser = CSVFormat.DEFAULT.parse(new InputStreamReader(inputStream))) {
            return csvParser.getRecords();
        }
    }

    private void insertRecordsIntoPostgres(Connection connection, String tableName, List<CSVRecord> records)
            throws SQLException {
        if (records.isEmpty()) {
            System.out.println("No records to insert.");
            return;
        }

        String createTableQuery = buildCreateTableQuery(tableName, records.get(0));
        executeQuery(connection, createTableQuery);

        String insertQuery = buildInsertQuery(tableName, records.get(0));
        executeInsert(connection, insertQuery, records);
    }

    private String buildCreateTableQuery(String tableName, CSVRecord csvRecord) {
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE " + tableName + " (");

        for (int i = 0; i < csvRecord.size(); i++) {
            createTableQuery.append(csvRecord.get(i)).append(" VARCHAR(255), ");
        }

        createTableQuery.setLength(createTableQuery.length() - 2);
        createTableQuery.append(")");

        return createTableQuery.toString();
    }

    private void executeQuery(Connection connection, String query) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.executeUpdate();
        }
    }

    private String buildInsertQuery(String tableName, CSVRecord csvRecord) {
        StringBuilder insertQuery = new StringBuilder("INSERT INTO " + tableName + " VALUES (");

        for (int i = 0; i < csvRecord.size(); i++) {
            insertQuery.append("?, ");
        }

        insertQuery.setLength(insertQuery.length() - 2);
        insertQuery.append(")");

        return insertQuery.toString();
    }

    private void executeInsert(Connection connection, String insertQuery, List<CSVRecord> records) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
            for (CSVRecord record : records) {
                for (int i = 0; i < record.size(); i++) {
                    preparedStatement.setString(i + 1, record.get(i));
                }
                preparedStatement.executeUpdate();
            }
        }
    }
}
