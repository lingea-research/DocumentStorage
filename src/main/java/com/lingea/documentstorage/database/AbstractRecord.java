package com.lingea.documentstorage.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Map;

public abstract class AbstractRecord {
    // This is only for SQL
    private final String type;

    private final int id;
    private final String hash;

    protected AbstractRecord(String type, int id, String hash) {
        this.type = type;
        this.id = id;
        this.hash = hash;
    }

    public int getId() {
        return this.id;
    }

    public String getHash() {
        return this.hash;
    }

    protected String getType() {
        return this.type;
    }

    public void delete(Connection conn) throws SQLException, IOException {
        delete(this.type, conn, this.getId());
    }

    protected static void delete(String type, Connection conn, int id) throws IOException, SQLException {
        String sql = "DELETE FROM " + type + " WHERE id = (?)";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, id);
        stmt.executeUpdate();
    }

    protected static void delete(String type, Connection conn, String hash) throws IOException, SQLException {
        String sql = "DELETE FROM " + type + " WHERE hash = (?)";

        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, hash);
        stmt.executeUpdate();
    }

    public static String getSelectQuery(String what, String from, String field, AbstractRecord[] records,
            Map<Integer, Integer> ids) {
        StringBuilder builder = new StringBuilder("SELECT ");
        builder.append(what);
        builder.append(" FROM ");
        builder.append(from);
        builder.append(" WHERE ");
        builder.append(field);
        builder.append(" IN (");
        for (int i = 0; i < records.length; i++) {
            builder.append("?");
            if (i != records.length - 1) {
                builder.append(",");
            }
            ids.put(records[i].getId(), i);
        }
        builder.append(")");
        return builder.toString();
    }

    public static <T> void fillIds(Connection conn, String query, AbstractRecord[] records, Map<Integer, T> ids,
            String idName) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(query)) {
            for (int i = 0; i < records.length; i++) {
                statement.setString(i + 1, records[i].getHash());
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int id = resultSet.getInt(idName);
                    ids.put(id, null);
                }
            }
        }
    }

    public static void insert(Connection conn, Map<Integer, Integer> ids, String tableName, String parent, String child,
            int parentId) throws SQLException {
        String sql = "INSERT INTO " + tableName + " (" + parent + ", " + child + ", position) VALUES (?, ?, ?)";
        PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        for (int id : ids.keySet()) {
            if (ids.get(id) == null) {
                continue;
            }
            stmt.setInt(1, parentId);
            stmt.setInt(2, id);
            stmt.setInt(3, ids.get(id));
            stmt.addBatch();
        }
        try {
            stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
