package com.lingea.documentstorage.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

enum Converter {
    ParagraphOfSentence,
    DocumentOfParagraph,
    SentenceOccurrence, // This is Sentence -> Document, just awfully named
    DocumentToOccurrence,
    SentenceToOccurrence,
    ParagraphToOccurrence,
    None;

    public static Converter getConverter(MapLevel in, MapLevel out) {
        in = MapLevel.values()[Math.min(in.ordinal(), MapLevel.OCCURRENCE.ordinal())];
        out = MapLevel.values()[Math.min(out.ordinal(), MapLevel.OCCURRENCE.ordinal())];

        int distance = Math.abs(in.ordinal() - out.ordinal());
        if (distance == 0) {
            // Should never happen though?
            return None;
        }

        if (distance == 1) {
            if (in.equals(MapLevel.SENTENCE) || out.equals(MapLevel.SENTENCE)) {
                return ParagraphOfSentence;
            }

            if (in.equals(MapLevel.OCCURRENCE) || out.equals(MapLevel.OCCURRENCE)) {
                return DocumentToOccurrence;
            }

            return DocumentOfParagraph;
        }

        if (distance == 2) {
            if (in.equals(MapLevel.SENTENCE) || out.equals(MapLevel.SENTENCE)) {
                return SentenceOccurrence;
            }

            return ParagraphToOccurrence;
        }

        return SentenceToOccurrence;
    }
}

public class DocumentMapper {
    public static List<Map<String, String>> executeNonOver(Connection conn, String[] inValues, 
                String inType, String[] outType, MapLevel inLevel, MapLevel outLevel, boolean includeOrigin) throws SQLException {

        String select = getSelectClause(inLevel, inType, outLevel, outType, includeOrigin);
        String from = getFromClause(inLevel, outLevel);
        String where = "WHERE ";
        if (inValues.length > 0) {
            where += buildParam(inLevel, inType) + " IN " + concatValues(inValues) + "AND " + getWhereClauseAppendix(inLevel, outLevel);
        } else {
            where += getWhereClauseAppendix(inLevel, outLevel);
        }
        String query = select + " " + from + " " + where + ";";

        PreparedStatement stmt = conn.prepareStatement(query);

        // IN
        for (int i = 0; i < inValues.length; i++) {
            stmt.setString(1 + i, inValues[i]);
        }

        ResultSet rs = stmt.executeQuery();

        List<Map<String, String>> resultList = new LinkedList<>();
        while (rs.next()) {
            Map<String, String> map = new HashMap<>();
            if (includeOrigin) {
                map.put(buildParam(inLevel, inType), rs.getString(1));
            } 
            for (int i = 0; i < outType.length; i++) {
                map.put(outType[i], rs.getString((includeOrigin ? 2 : 1) + i));
            }
            resultList.add(map);
        }

        stmt.close();

        return resultList;        
    }

    public static List<Map<String, String>> executeOver(Connection conn, String[] inValues, 
                String inType, String[] outType, MapLevel inLevel, MapLevel outLevel, 
                MapLevel overLevel, boolean includeOrigin) throws SQLException {

        List<Map<String, String>> result = executeNonOver(conn, inValues, inType, new String[]{"id"}, inLevel, overLevel, includeOrigin);

        Set<String> ids = new HashSet<String>();
        for (Map<String, String> map : result) {
            ids.add(map.get("id"));
        }

        return executeNonOver(conn, ids.toArray(new String[0]), "id", 
                                                outType, overLevel, outLevel, includeOrigin);
    }

    private static String getSelectClause(MapLevel inLevel, String inType, MapLevel outLevel, String[] outType, boolean includeOrigin) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT DISTINCT ");
        if (includeOrigin) {
            builder.append(buildParam(inLevel, inType));
            for (int i = 0; i < outType.length; i++) {
                builder.append(", " + buildParam(outLevel, outType[i]));
            }
        } else {
            builder.append(buildParam(outLevel, outType[0]));
            for (int i = 1; i < outType.length; i++) {
                builder.append(", " + buildParam(outLevel, outType[i]));
            }
        }

        return builder.toString();
    }

    private static String getFromClause(MapLevel inLevel, MapLevel outLevel) {
        StringBuilder builder = new StringBuilder();
        builder.append("FROM ");
        builder.append(inLevel);
        builder.append(", " + outLevel);
        builder.append(getFromByConverter(Converter.getConverter(inLevel, outLevel)));

        return builder.toString();
    }

    private static String concatValues(String[] values) {
        StringBuilder builder = new StringBuilder();
        builder.append("(");
        for (int i = 0; i < values.length - 1; i++) {
            builder.append("?, ");
        }
        if (values.length > 0) {
            builder.append("?");
        }
        builder.append(")");

        return builder.toString();
    }

    private static String getFromByConverter(Converter converter) {
        StringBuilder builder = new StringBuilder();
        switch (converter) {
            case None:
                break;
            case ParagraphOfSentence:
                builder.append(", ParagraphOfSentence");
                break;
            case DocumentOfParagraph:
                builder.append(", DocumentOfParagraph");
                break;
            case SentenceOccurrence:
                builder.append(", SentenceOccurrence");
                break;
            case DocumentToOccurrence:
                builder.append(", Occurrence");
                break;
            case ParagraphToOccurrence:
                builder.append(", DocumentOfParagraph, Occurrence");
                break;
            case SentenceToOccurrence:
                builder.append(", SentenceOccurrence, Occurrence");
                break;
        }

        return builder.toString();
    }

    private static String buildParam(MapLevel level, String type) {
        return level + "." + type;
    }

    private static String getWhereClauseAppendix(MapLevel inLevel, MapLevel outLevel) {
        StringBuilder builder = new StringBuilder();
        builder.append(getWhereByConverter(Converter.getConverter(inLevel, outLevel)));

        if (inLevel.equals(MapLevel.URL) || outLevel.equals(MapLevel.URL)) {
            builder.append(" AND Url.id = Occurrence.url");
        }

        return builder.toString();
    }

    private static String getWhereByConverter(Converter converter) {
        StringBuilder builder = new StringBuilder();
        switch (converter) {
            case None:
                break;
            case ParagraphOfSentence:
                builder.append("Sentence.id = ParagraphOfSentence.sentence");
                builder.append(" AND ParagraphOfSentence.paragraph = Paragraph.id");
                break;
            case DocumentOfParagraph:
                builder.append("Paragraph.id = DocumentOfParagraph.paragraph");
                builder.append(" AND DocumentOfParagraph.document = Document.id");
                break;
            case SentenceOccurrence:
                builder.append("Sentence.id = SentenceOccurrence.sentence");
                builder.append(" AND SentenceOccurrence.document = Document.id");
                break;
            case DocumentToOccurrence:
                builder.append("Document.id = Occurrence.document");
                break;
            case ParagraphToOccurrence:
                builder.append("Paragraph.id = DocumentOfParagraph.paragraph");
                builder.append(" AND DocumentOfParagraph.document = Occurrence.document");
                break;
            case SentenceToOccurrence:
                builder.append("Sentence.id = SentenceOccurrence.sentence");
                builder.append(" AND SentenceOccurrence.document = Occurrence.document");
                break;
        }

        return builder.toString();
    }
}