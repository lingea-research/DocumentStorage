package com.lingea.documentstorage.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class ParagraphOfSentence {
    private final int paragraphId;
    private final int sentenceId;
    private final int position;

    ParagraphOfSentence(int paragraphId, int sentenceId, int position) {
        this.paragraphId = paragraphId;
        this.sentenceId = sentenceId;
        this.position = position;
    }

    public static boolean exists(Connection conn, int paragraphId, int sentenceId) throws SQLException {
        String sql = "SELECT 1 FROM ParagraphOfSentence WHERE paragraph = (?) AND sentence = (?)";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, paragraphId);
        stmt.setInt(2, sentenceId);

        ResultSet rs = stmt.executeQuery();

        // For some reason this buffer is very important!
        boolean ret = rs.next();

        stmt.close();

        return ret;
    }

    public static ParagraphOfSentence create(Connection conn, int paragraphId, int sentenceId, int position)
            throws IOException, SQLException {
        String sql = "INSERT INTO ParagraphOfSentence (paragraph, sentence, position) VALUES (?, ?, ?)";

        PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        stmt.setInt(1, paragraphId);
        stmt.setInt(2, sentenceId);
        stmt.setInt(3, position);

        stmt.executeUpdate();
        ResultSet rs = stmt.getGeneratedKeys();
        rs.next();

        stmt.close();

        return new ParagraphOfSentence(paragraphId, sentenceId, position);
    }

    public static void createMany(Connection conn, ParagraphRecord[] paragraphRecords,
            List<List<SentenceRecord>> sentenceRecords)
            throws SQLException {

        String sql = "INSERT OR IGNORE INTO ParagraphOfSentence (paragraph, sentence, position) VALUES (?, ?, ?)";

        PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

        for (int i = 0; i < sentenceRecords.size(); i++) {
            for (int j = 0; j < sentenceRecords.get(i).size(); j++) {
                stmt.setInt(1, paragraphRecords[i].getId());
                stmt.setInt(2, sentenceRecords.get(i).get(j).getId());
                stmt.setInt(3, j);
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
