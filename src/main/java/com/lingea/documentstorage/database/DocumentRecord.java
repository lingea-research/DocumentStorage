package com.lingea.documentstorage.database;

import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

import com.lingea.documentstorage.exception.RecordAlreadyExistsException;
import com.lingea.documentstorage.exception.RecordNotFoundException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DocumentRecord extends AbstractRecord {
    private static final String TYPE = "Document";
    private final List<OccurrenceRecord> occurrences;

    private DocumentRecord(int id, String hash, List<OccurrenceRecord> occurrences) {
        super(TYPE, id, hash);
        this.occurrences = occurrences;
    }

    private DocumentRecord(int id, String hash) {
        this(id, hash, new ArrayList<OccurrenceRecord>());
    }

    /**
     * Creates new Document Record in Document table
     * @param conn connection
     * @param hash document hash
     * @return
     * @throws SQLException
     * @throws RecordAlreadyExistsException
     * @throws RecordAlreadyExists
     */
    public static DocumentRecord create(Connection conn, String hash) throws SQLException, RecordAlreadyExistsException {
        String sql = "INSERT INTO Document (hash) VALUES (?)";
        PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        stmt.setString(1, hash);

        try {
            stmt.executeUpdate();
        } catch (SQLiteException e) {
            System.out.println("EXECUTE UPDATE ERROR " + e.getMessage());
            if (Objects.equals(e.getResultCode().toString(), SQLiteErrorCode.getErrorCode(2067).toString())) {
                throw new RecordAlreadyExistsException(String.format("Document(hash=%s)", hash));
            } else {
                throw e;
            }
        }
        
        ResultSet rs = stmt.getGeneratedKeys();
        rs.next();
        int docId = rs.getInt(1);
        rs.close();
        
        return new DocumentRecord(docId, hash);
    }

    public static DocumentRecord getById(Connection conn, int id) 
            throws SQLException, IOException, RecordNotFoundException {

        String sql = "SELECT D.id, D.hash, O.time, O.indexerId, U.url, U.id FROM Document D\n" +
                    "LEFT JOIN Occurrence O ON D.id = O.document\n" +
                    "LEFT JOIN Url U ON U.id = O.url\n" +
                    "WHERE D.id = ?";

        
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, id);

        return genericGetDocumentRecord(conn, stmt);
    }

    /**
     * Retrieves DocumentRecord by it's hash
     * @param conn
     * @param hash
     * @return
     * @throws SQLException
     * @throws IOException
     * @throws RecordNotFoundException
     * @throws DatabaseException.RecordNotFound
     */
    public static DocumentRecord getByHash(Connection conn, String hash)
            throws SQLException, IOException, RecordNotFoundException {

        String sql = "SELECT D.id, D.hash, O.time, O.indexerId, U.url, U.id FROM Document D\n" +
                    "LEFT JOIN Occurrence O ON D.id = O.document\n" +
                    "LEFT JOIN Url U ON U.id = O.url\n" +
                    "WHERE hash = ?";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, hash);

        return genericGetDocumentRecord(conn, stmt);
    }

    /**
     * Returns document by url between low and high
     * @throws RecordNotFoundException
     */
    public static DocumentRecord getByUrlAndTime(Connection conn, String url, long lowTime, long highTime)
            throws SQLException, IOException, RecordNotFoundException {

        String sql = "SELECT D.id, D.hash, O.time, O.indexerId, U.url, U.id FROM Document D\n" +
                    "LEFT JOIN Occurrence O ON D.id = O.document\n" +
                    "LEFT JOIN Url U ON U.id = O.url\n" +
                    "WHERE U.url = ? AND time BETWEEN ? AND ?";
        
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, url);
        stmt.setLong(2, lowTime);
        stmt.setLong(3, highTime);

        return genericGetDocumentRecord(conn, stmt);
    }

    public static DocumentRecord getClosestByUrl(Connection conn, String url, long timestamp) 
            throws IOException, SQLException, RecordNotFoundException {

        String sql = "SELECT D.id, D.hash, O.time, O.indexerId, U.url, U.id FROM Occurrence O\n" +
                "LEFT JOIN Url U ON U.id = O.url\n" + 
                "LEFT JOIN Document D ON D.id = O.document\n" +
                "WHERE U.url = ? ORDER BY abs(? - time)\n" +
                "LIMIT 1";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, url);
        stmt.setLong(2, timestamp);

        return genericGetDocumentRecord(conn, stmt);
    }

    public static DocumentRecord getClosestBefore(Connection conn, String url, long timestamp) 
            throws IOException, SQLException, RecordNotFoundException {

        String sql = "SELECT D.id, D.hash, O.time, O.indexerId, U.url, U.id FROM Occurrence O\n" +
                "LEFT JOIN Url U ON U.id = O.url\n" + 
                "LEFT JOIN Document D ON D.id = O.document\n" +
                "WHERE U.url = ? AND O.time <= ? ORDER BY abs(? - time)\n" +
                "LIMIT 1";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, url);
        stmt.setLong(2, timestamp);
        stmt.setLong(3, timestamp);

        return genericGetDocumentRecord(conn, stmt);
    }

    public static DocumentRecord getClosestAfter(Connection conn, String url, long timestamp) 
            throws IOException, SQLException, RecordNotFoundException {

        String sql = "SELECT D.id, D.hash, O.time, O.indexerId, U.url, U.id FROM Occurrence O\n" +
                "LEFT JOIN Url U ON U.id = O.url\n" + 
                "LEFT JOIN Document D ON D.id = O.document\n" +
                "WHERE U.url = ? AND O.time >= ? ORDER BY abs(? - time)\n" +
                "LIMIT 1";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, url);
        stmt.setLong(2, timestamp);
        stmt.setLong(3, timestamp);

        return genericGetDocumentRecord(conn, stmt);
    }

    /**
     * Generates single document record returned by executing sql with arguments  
     * First part of the query should always look like this:  
     * String sql = "SELECT D.id, D.hash, O.time, O.indexerId, U.url, U.id FROM Document D\n" +  
     *              "LEFT JOIN Occurrence O ON D.id = O.document\n" +  
     *              "LEFT JOIN Url U ON U.id = O.url\n" +  
     * @param conn Connection
     * @param sql SQL query
     * @param args Arguments for the SQL query
     * @return Single DocumentRecord or exception
     * @throws SQLException
     * @throws IOException
     * @throws RecordNotFoundException
     * @throws DatabaseException.RecordNotFound
     */
    private static DocumentRecord genericGetDocumentRecord(Connection conn, PreparedStatement stmt)
            throws SQLException, IOException, RecordNotFoundException {
        ResultSet rs = stmt.executeQuery();

        if (!rs.next()) {
            throw new RecordNotFoundException("Error while processing statement " + stmt);
        }

        // Document
        int documentId = rs.getInt(1);
        String documentHash = rs.getString(2);
        List<OccurrenceRecord> occurs = new ArrayList<>();
        do {
            // Occurance
            long time = rs.getLong(3);
            String indexer = rs.getString(4);

            // Url
            String url = rs.getString(5);
            int urlId = rs.getInt(6);

            Url realUrl = new Url(urlId, url);
            occurs.add(new OccurrenceRecord(documentId, realUrl, indexer, time));
        } while (rs.next());

        stmt.close();
        return new DocumentRecord(documentId, documentHash, occurs);
    }

    public static void update(Connection conn, int id, String hash) throws IOException, SQLException {
        String sql = "UPDATE Document set hash = ? WHERE id = ?";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, hash);
        stmt.setInt(2, id);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void delete(Connection conn, int id) throws IOException, SQLException {
        AbstractRecord.delete(TYPE, conn, id);
    }

    public static void delete(Connection conn, String hash) throws IOException, SQLException {
        AbstractRecord.delete(TYPE, conn, hash);
    }

    public List<OccurrenceRecord> getOccurrences() {
        return Collections.unmodifiableList(this.occurrences);
    }

    public void addOccurrence(OccurrenceRecord occurrence) {
        this.occurrences.add(occurrence);
    }
}
