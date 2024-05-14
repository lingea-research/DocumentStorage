package com.lingea.documentstorage.database;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

import com.lingea.documentstorage.exception.RecordNotFoundException;

import java.util.ArrayList;
import java.io.IOException;
import java.sql.*;

public class OccurrenceRecord {
    private final int documentId;
    private final long time;
    private final Url url;
    private final String indexerId;

    OccurrenceRecord(int documentId, Url url, String indexerId, long time) {
        this.documentId = documentId;
        this.url = url;
        this.indexerId = indexerId;
        this.time = time;
    }

    public static OccurrenceRecord create(Connection conn, Url url, int documentId, String indexerId, Clock clock) throws IOException, SQLException {
        String sql = "INSERT INTO Occurrence (url, document, time, indexerId) VALUES (?, ?, ?, ?)";

        long nowDate = Instant.now(clock).getEpochSecond();

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, url.getId());
        stmt.setInt(2, documentId);
        stmt.setLong(3, nowDate);
        stmt.setString(4, indexerId);
        stmt.executeUpdate();
        stmt.close();

        return new OccurrenceRecord(documentId, url, indexerId, nowDate);
    }

    /**
     * Retrieves all occurrenceRecords by document id
     * @param conn Database connection
     * @param documentId id of a document
     * @return occurrenceRecords for documentId
     * @throws IOException
     * @throws SQLException
     * @throws RecordNotFoundException
     * @throws RecordNotFound
     */
    public static List<OccurrenceRecord> getByDocumentId(Connection conn, int documentId) throws IOException, SQLException, RecordNotFoundException {
        String sql = "SELECT document, url, time, indexerId FROM Occurrence WHERE document = ?";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, documentId);
        return genericGet(conn, stmt);
    }

    private static List<OccurrenceRecord> genericGet(Connection conn, PreparedStatement stmt) throws IOException, SQLException, RecordNotFoundException {
        ResultSet rs = stmt.executeQuery();
        List<OccurrenceRecord> records = new ArrayList<OccurrenceRecord>();

        while (rs.next()) {
            int url = rs.getInt("url");
            Url realUrl = Url.getById(conn, url);
            long time = rs.getLong("time");
            String indexerId = rs.getString("indexerId");
            int documentId = rs.getInt("document");

            records.add(new OccurrenceRecord(documentId, realUrl, indexerId, time));
        }
        rs.close();
        stmt.close();

        return records;
    }

    public static void update(Connection conn, int id_, String hash) throws IOException, SQLException {
        String sql = "UPDATE Occurrence set hash = ? WHERE id = ?";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, hash);
        stmt.setInt(2, id_);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void delete(Connection conn, int id) throws IOException, SQLException {
        String sql = "DELETE FROM Occurrence WHERE id = (?)";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, id);
        stmt.executeUpdate();
        stmt.close();
    }

    public long getTime() {
        return time;
    }

    public Url getUrl() {
        return url;
    }

    public String getIndexerId() {
        return indexerId;
    }
}
