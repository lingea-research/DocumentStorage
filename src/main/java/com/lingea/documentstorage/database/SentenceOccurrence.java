package com.lingea.documentstorage.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.lingea.documentstorage.DocumentMeta;

public class SentenceOccurrence {

    public static void create(Connection conn, int sentenceId, int docId, int paragraphId,
            int documentPosition, int paragraphPosition)
            throws IOException, SQLException {

        String sql = "INSERT INTO SentenceOccurrence (sentence, document, paragraph, documentPos, paragraphPos) VALUES (?, ?, ?, ?, ?)";

        PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        stmt.setInt(1, sentenceId);
        stmt.setInt(2, docId);
        stmt.setInt(3, paragraphId);
        stmt.setInt(4, documentPosition);
        stmt.setInt(5, paragraphPosition);

        stmt.executeUpdate();
        ResultSet rs = stmt.getGeneratedKeys();
        rs.next();

        stmt.close();
    }

    public static void createMany(Connection conn, DocumentMeta doc, ParagraphRecord[] paragraphRecords,
            List<List<SentenceRecord>> sentenceRecords)
            throws SQLException {

        String sql = "INSERT OR IGNORE INTO SentenceOccurrence (sentence, document, paragraph, documentPos, paragraphPos) VALUES (?, ?, ?, ?, ?)";

        PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

        for (int i = 0; i < sentenceRecords.size(); i++) {
            for (int j = 0; j < sentenceRecords.get(i).size(); j++) {
                stmt.setInt(1, sentenceRecords.get(i).get(j).getId());
                stmt.setInt(2, doc.id);
                stmt.setInt(3, paragraphRecords[i].getId());
                stmt.setInt(4, i);
                stmt.setInt(5, j);
                stmt.addBatch();
            }
        }

        try {
            stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
