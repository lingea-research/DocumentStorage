package com.lingea.documentstorage.database;

import java.io.IOException;
import java.sql.*;
import java.util.Objects;

import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

import com.lingea.documentstorage.exception.RecordAlreadyExistsException;
import com.lingea.documentstorage.exception.RecordNotFoundException;

public class Url {
    private final int id;
    private final String url;

    Url(int id, String url) {
        this.url = url;
        this.id = id;
    }

    public static Url create(Connection conn, String url) throws IOException, SQLException, RecordAlreadyExistsException {
        String sql = "INSERT INTO Url (url) VALUES (?)";

        PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        stmt.setString(1, url);

        try {
            stmt.executeUpdate();
        } catch (SQLiteException e) {
            System.out.println("EXECUTE UPDATE ERROR " + e.getMessage());
            if (Objects.equals(e.getResultCode().toString(), SQLiteErrorCode.getErrorCode(2067).toString())) {
                throw new RecordAlreadyExistsException(String.format("Url(url=%s)", url));
            } else {
                throw e;
            }
        }

        ResultSet rs = stmt.getGeneratedKeys();
        rs.next();

        int urlId = rs.getInt(1);
        rs.close();
        stmt.close();
        return new Url(urlId, url);
    }

     /**
      * Creates Url object based on the id url id
      * @param conn Database connection
      * @param id Url id
      * @return Url object
      * @throws IOException
      * @throws SQLException
     * @throws RecordNotFoundException
      * @throws RecordNotFound
      */
    public static Url getById(Connection conn, int id) throws IOException, SQLException, RecordNotFoundException {
        String sql = "SELECT url FROM url WHERE id = ?";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, id);
        ResultSet rs = stmt.executeQuery();

        if (!rs.next()) {
            throw new RecordNotFoundException("Url with id " + id + " not found");
        }

        String url = rs.getString("url");
        rs.close();
        stmt.close();
        return new Url(id, url);
    }

    /**
     * Returns url by String
     * @param conn Connection
     * @param url string url
     * @return Url object
     * @throws SQLException
     * @throws IOException
     * @throws RecordNotFoundException
     * @throws RecordNotFound
     */
    public static Url getByUrl(Connection conn, String url) throws SQLException, IOException, RecordNotFoundException {
        String sql = "SELECT id FROM url WHERE url = ?";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, url);
        ResultSet rs = stmt.executeQuery();

        if (!rs.next()) {
            throw new RecordNotFoundException("Url with address " + url + " not found");
        }

        int docHash = rs.getInt(1);
        rs.close();
        stmt.close();
        return new Url(docHash, url);
    }

    public static void update(Connection conn, int id_, String url) throws IOException, SQLException {
        String sql = "UPDATE Url SET url = ? WHERE id = ?";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, url);
        stmt.setInt(2, id_);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void delete(Connection conn, int id_) throws IOException, SQLException {
        String sql = "DELETE FROM Url WHERE id = (?)";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, id_);
        stmt.executeUpdate();
        stmt.close();
    }

    public int getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }
}
