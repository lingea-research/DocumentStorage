package com.lingea.documentstorage.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Map;
import java.util.HashMap;

import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

import com.lingea.documentstorage.exception.RecordAlreadyExistsException;
import com.lingea.documentstorage.exception.RecordNotFoundException;

public class ParagraphRecord extends AbstractRecord {
    private static final String TYPE = "Document";

    private ParagraphRecord(int id, String hash) {
        super(TYPE, id, hash);
    }

    public static ParagraphRecord create(Connection conn, int documentId, int position, String hash)
            throws SQLException, IOException, RecordAlreadyExistsException {
        String sql = "INSERT INTO Paragraph (hash) VALUES (?)";
        PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        stmt.setString(1, hash);

        try {
            stmt.executeUpdate();
        } catch (SQLiteException e) {
            System.out.println("EXECUTE UPDATE ERROR " + e.getMessage());
            if (Objects.equals(e.getResultCode().toString(), SQLiteErrorCode.getErrorCode(2067).toString())) {
                throw new RecordAlreadyExistsException(String.format("Paragraph(hash=%s)", hash));
            } else {
                throw e;
            }
        }

        ResultSet rs = stmt.getGeneratedKeys();
        rs.next();
        int parId = rs.getInt(1);
        rs.close();

        return new ParagraphRecord(parId, hash);
    }

    /** 
     * Method for inserting multiple paragraphs at once. 
     * Returns an array of paragraph records of the same size as the hashes input array.
     */
    public static ParagraphRecord[] createMany(Connection conn, String[] hashes)
            throws SQLException, IOException {

        insertMany(conn, hashes);

        Map<String, Integer> resultMap = getUniqueHashes(conn, hashes); // result map = hash -> id

        ParagraphRecord[] result = new ParagraphRecord[hashes.length];
        for (int i = 0; i < hashes.length; i++) {
            System.out.println(resultMap.get(hashes[i]) + " - " + hashes[i]);
            result[i] = new ParagraphRecord(resultMap.get(hashes[i]), hashes[i]);
        }

        return result;
    }

    private static void insertMany(Connection conn, String[] hashes) throws SQLException {
        String sql = "INSERT INTO Paragraph (hash) SELECT ? WHERE NOT EXISTS " +
                "(SELECT hash FROM Paragraph WHERE hash = ?)";

        PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        for (int i = 0; i < hashes.length; i++) {
            stmt.setString(1, hashes[i]);
            stmt.setString(2, hashes[i]);
            stmt.addBatch();
        }
        try {
            stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static Map<String, Integer> getUniqueHashes(Connection conn, String[] hashes) throws SQLException {
        StringBuilder builder = new StringBuilder("SELECT id, hash FROM Paragraph WHERE hash in (");
        for (int i = 0; i < hashes.length; i++) {
            builder.append("?");
            if (i < hashes.length - 1) {
                builder.append(", ");
            }
        }
        builder.append(")");

        PreparedStatement stmt = conn.prepareStatement(builder.toString());

        for (int i = 0; i < hashes.length; i++) {
            stmt.setString(i + 1, hashes[i]);
        }

        ResultSet rs;
        try {
            rs = stmt.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        Map<String, Integer> resultMap = new HashMap<>();
        while (rs.next()) {
            resultMap.put(rs.getString("hash"), rs.getInt("id"));
        }
        return resultMap;
    }

    // TODO: PoC, currently unused, maybe we would wish to use this in the future for
    // remapping up and down via indexingService /similar endpoint. 
    // (eg. search level Paragraph -> result level Document)
    public static ParagraphRecord getBySentence(Connection conn, int id)
            throws SQLException, IOException, RecordNotFoundException {
        String sql = "SELECT P.id, P.hash FROM Paragraph P\n" +
                "LEFT JOIN ParagraphOfSentence POS on P.id = POS.paragraph\n" +
                "LEFT JOIN Sentence S on POS.sentence = S.id\n" +
                "WHERE S.id = ?";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, id);

        ResultSet rs = stmt.executeQuery();

        if (!rs.next()) {
            throw new RecordNotFoundException("Can't find paragraph of sentence: " + id);
        }

        // paragraph
        int pId = rs.getInt(1);
        String pHash = rs.getString(2);

        return new ParagraphRecord(pId, pHash);
    }

    public static ParagraphRecord getById(Connection conn, int id) 
        throws SQLException, IOException, RecordNotFoundException {

        String sql = "SELECT P.id, P.hash FROM Paragraph P\n" +
                    "WHERE id = ?";

        return genericGetParagraphRecord(conn, sql, new String[]{ Integer.toString(id) });
    }

    public static ParagraphRecord getByHash(Connection conn, String hash)
            throws SQLException, IOException, RecordNotFoundException {
        String sql = "SELECT P.id, P.hash FROM Paragraph P\n" +
                "WHERE hash = ?";

        return genericGetParagraphRecord(conn, sql, new String[] { hash });
    }

    private static ParagraphRecord genericGetParagraphRecord(Connection conn, String sql, String[] args)
            throws SQLException, IOException, RecordNotFoundException {
        PreparedStatement stmt = conn.prepareStatement(sql);
        for (int i = 0; i < args.length; i++) {
            stmt.setString(i + 1, args[i]);
        }
        ResultSet rs = stmt.executeQuery();

        if (!rs.next()) {
            throw new RecordNotFoundException(
                    "Error while processing query " + sql + " with arguments, " + String.join(", ", args));
        }

        // Paragraph
        int paragraphId = rs.getInt(1);
        String paragraphHash = rs.getString(2);

        stmt.close();

        return new ParagraphRecord(paragraphId, paragraphHash);
    }

    public static void update(Connection conn, int id, String hash) throws IOException, SQLException {
        String sql = "UPDATE Paragraph SET hash = ? WHERE id = ?";

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
}
