package com.lingea.documentstorage.utils;

import java.sql.DriverManager;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DBInitializator {
    private static final String DATABASE_NAME = "documentStorage.db";
    private static final String DATABASE_POPULATE_FILE = "create_database.sql";

    private final String dirPath;

    public DBInitializator(String dirPath) throws SQLException, IOException {
        this.dirPath = dirPath;

        File dbFile = new File(dirPath + DBInitializator.DATABASE_NAME);

        if (!dbFile.exists()) {
            populate();
            createIndices();
        }
    }

    private void populate() throws IOException, SQLException {
        Connection conn = this.getConnection();
        ScriptRunner scriptRunner = new ScriptRunner(conn, false, true);

        InputStream inputStream = DBInitializator.class.getClassLoader().getResourceAsStream(DBInitializator.DATABASE_POPULATE_FILE);
        // the stream holding the file content
        if (inputStream == null) {
            throw new IllegalArgumentException("File not found!: " + DBInitializator.DATABASE_POPULATE_FILE + "\n");
        }
        byte[] content = new byte[1024];
        int bytesRead = 0;
        StringBuilder strFileContents = new StringBuilder();
        while ((bytesRead = inputStream.read(content)) != -1) {
            strFileContents.append(new String(content, 0, bytesRead));
        }
        Reader reader = new StringReader(strFileContents.toString());

        try {
            System.out.print("Creating database structure!\n");
            scriptRunner.runScript(reader);
            conn.close();
        } catch (Exception e) {
            System.out.print("Error occurred when trying create database structure!\n");
            System.out.print(e.getMessage());
            conn.close();
        }

    }

    private void createIndices() throws SQLException, IOException {
        System.out.println("Creating indices.");

        String[] queries = {
                "CREATE INDEX IF NOT EXISTS ParagraphIndex ON Paragraph (hash)",
                "CREATE INDEX IF NOT EXISTS SentenceIndex ON Sentence (hash)",
                /*"CREATE INDEX IF NOT EXISTS PoSIndex ON ParagraphOfSentence (paragraph, sentence)",
                "CREATE INDEX IF NOT EXISTS DoPIndex ON DocumentOfParagraph (document, paragraph)",
                "CREATE INDEX IF NOT EXISTS Dop2Index ON DocumentOfParagraph (paragraph, sentence, position)",
                "CREATE INDEX IF NOT EXISTS SOIndex ON SentenceOccurrence (document, paragraph, sentence)"*/
        };

        Connection conn = getConnection();

        for (String query : queries) {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(query);
        }

        conn.close();
    }

    public Connection getConnection() throws SQLException, IOException {
        Connection conn;
        String dbUrl = "jdbc:sqlite:" + this.dirPath + DBInitializator.DATABASE_NAME;
        conn = DriverManager.getConnection(dbUrl);
        System.out.println("Connection to SQLite has been established.");
        return conn;
    }
}
