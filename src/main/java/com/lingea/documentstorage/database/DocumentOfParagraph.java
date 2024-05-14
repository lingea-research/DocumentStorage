package com.lingea.documentstorage.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DocumentOfParagraph {
    private final int documentId;
    private final int paragraphId;
    private final int position;

    DocumentOfParagraph(int documentId, int paragraphId, int position) {
        this.documentId = documentId;
        this.paragraphId = paragraphId;
        this.position = position;
    }

    public static boolean exists(Connection conn, int docId, int paragraphId) throws SQLException {
        String sql = "SELECT 1 FROM DocumentOfParagraph WHERE document = (?) AND paragraph = (?)";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, docId);
        stmt.setInt(2, paragraphId);

        ResultSet rs = stmt.executeQuery();

        // For some reason this buffer is very important!
        boolean ret = rs.next();

        stmt.close();

        return ret;
    }

    public static DocumentOfParagraph create(Connection conn, int docId, int paragraphId, int position)
            throws IOException, SQLException {
        String sql = "INSERT INTO DocumentOfParagraph (document, paragraph, position) VALUES (?, ?, ?)";

        PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        stmt.setInt(1, docId);
        stmt.setInt(2, paragraphId);
        stmt.setInt(3, position);

        stmt.executeUpdate();
        ResultSet rs = stmt.getGeneratedKeys();
        rs.next();

        stmt.close();

        return new DocumentOfParagraph(docId, paragraphId, position);
    }

    public static void createMany(Connection conn, int docId, ParagraphRecord[] paragraphRecords)
            throws IOException, SQLException {

        String sql = "INSERT OR IGNORE INTO DocumentOfParagraph (document, paragraph, position) VALUES (?, ?, ?)";

        PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

        for (int i = 0; i < paragraphRecords.length; i++) {
            Integer parId = paragraphRecords[i].getId();
            stmt.setInt(1, docId);
            stmt.setInt(2, parId);
            stmt.setInt(3, i);
            stmt.addBatch();
        }

        try {
            stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
