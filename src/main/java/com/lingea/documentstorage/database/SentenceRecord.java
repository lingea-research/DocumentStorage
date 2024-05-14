package com.lingea.documentstorage.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

import com.lingea.documentstorage.exception.RecordAlreadyExistsException;
import com.lingea.documentstorage.exception.RecordNotFoundException;

public class SentenceRecord extends AbstractRecord {
    private static final String TYPE = "Sentence";

    private SentenceRecord(int id, String hash) {
        super(TYPE, id, hash);
    }

    public static SentenceRecord create(Connection conn, int paragraphId, int position, String hash)
            throws SQLException, IOException, RecordAlreadyExistsException {
        String sql = "INSERT INTO Sentence (hash) VALUES (?)";
        PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        stmt.setString(1, hash);

        try {
            stmt.executeUpdate();
        } catch (SQLiteException e) {
            System.out.println("EXECUTE UPDATE ERROR " + e.getMessage());
            if (e.getResultCode() == SQLiteErrorCode.getErrorCode(2067)) {
                throw new RecordAlreadyExistsException(String.format("Paragraph(hash=%s)", hash));
            } else {
                throw e;
            }
        }

        ResultSet rs = stmt.getGeneratedKeys();
        rs.next();
        int senId = rs.getInt(1);
        rs.close();

        ParagraphOfSentence.create(conn, paragraphId, senId, position);

        return new SentenceRecord(senId, hash);
    }

    public static List<List<SentenceRecord>> createMany(Connection conn, List<List<String>> hashes,
            ParagraphRecord[] paragraphRecords) throws SQLException {

        insertMany(conn, hashes);

        Map<String, Integer> resultMap = getUniqueHashes(conn, hashes); // result map = hash -> id

        List<List<SentenceRecord>> result = new ArrayList<>();
        for (int i = 0; i < paragraphRecords.length; i++) {
            result.add(new ArrayList<>());
        }

        for (int i = 0; i < hashes.size(); i++) {
            for (int j = 0; j < hashes.get(i).size(); j++) {
                result.get(i).add(new SentenceRecord(resultMap.get(hashes.get(i).get(j)), hashes.get(i).get(j)));
            }
        }

        return result;
    }

    private static void insertMany(Connection conn, List<List<String>> hashes) throws SQLException {
        String sql = "INSERT INTO Sentence (hash) SELECT ? WHERE NOT EXISTS " +
                "(SELECT hash FROM Sentence WHERE hash = ?)";

        PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

        for (int i = 0; i < hashes.size(); i++) {
            for (int j = 0; j < hashes.get(i).size(); j++) {
                stmt.setString(1, hashes.get(i).get(j));
                stmt.setString(2, hashes.get(i).get(j));
                stmt.addBatch();
            }
        }

        try {
            stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static Map<String, Integer> getUniqueHashes(Connection conn, List<List<String>> hashes)
            throws SQLException {
        StringBuilder builder = new StringBuilder("SELECT id, hash FROM Sentence WHERE hash IN (");
        for (int i = 0; i < hashes.size(); i++) {
            for (int j = 0; j < hashes.get(i).size(); j++) {
                builder.append("?");
                if (!(i == hashes.size() - 1 && j == hashes.get(i).size() - 1)) {
                    builder.append(", ");
                }
            }
        }
        builder.append(")");

        PreparedStatement stmt = conn.prepareStatement(builder.toString());

        int index = 1;
        for (int i = 0; i < hashes.size(); i++) {
            for (int j = 0; j < hashes.get(i).size(); j++) {
                stmt.setString(index, hashes.get(i).get(j));
                index++;
            }
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

    public static SentenceRecord getById(Connection conn, int id) 
            throws SQLException, IOException, RecordNotFoundException {
        
        String sql = "SELECT S.id, S.hash FROM Sentence S\n" +
                "WHERE id = ?";

        return genericGetSentenceRecord(conn, sql, new String[] { Integer.toString(id) });
    }

    public static SentenceRecord getByHash(Connection conn, String hash)
            throws SQLException, IOException, RecordNotFoundException {
        String sql = "SELECT S.id, S.hash FROM Sentence S\n" +
                "WHERE hash = ?";

        return genericGetSentenceRecord(conn, sql, new String[] { hash });
    }

    private static SentenceRecord genericGetSentenceRecord(Connection conn, String sql, String[] args)
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

        // Sentence
        int sentenceId = rs.getInt(1);
        String sentenceHash = rs.getString(2);

        stmt.close();

        return new SentenceRecord(sentenceId, sentenceHash);
    }

    public static void update(Connection conn, int id, String hash) throws IOException, SQLException {
        String sql = "UPDATE Sentence SET hash = ? WHERE id = ?";

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
