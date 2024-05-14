package com.lingea.documentstorage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.lingea.documentstorage.database.AbstractRecord;
import com.lingea.documentstorage.database.DocumentOfParagraph;
import com.lingea.documentstorage.database.DocumentRecord;
import com.lingea.documentstorage.database.OccurrenceRecord;
import com.lingea.documentstorage.database.ParagraphOfSentence;
import com.lingea.documentstorage.database.ParagraphRecord;
import com.lingea.documentstorage.database.SentenceOccurrence;
import com.lingea.documentstorage.database.SentenceRecord;
import com.lingea.documentstorage.database.Url;
import com.lingea.documentstorage.exception.RecordAlreadyExistsException;
import com.lingea.documentstorage.exception.RecordNotFoundException;
import com.lingea.documentstorage.io.Storage;
import com.lingea.documentstorage.recording.RecordChecksum;
import com.lingea.documentstorage.utils.DBInitializator;
import com.lingea.documentstorage.utils.DocumentMapper;
import com.lingea.documentstorage.utils.MapLevel;
import com.lingea.documentstorage.utils.StorageLevel;

/**
 * Class for manipulating the document storage.
 */
public class DocumentStorage {

    private DBInitializator dbInitializator;

    public Connection getConnection() throws SQLException, IOException {
        return dbInitializator.getConnection();
    }

    // TODO: use same document storage for everything, then we don't have to use Paragraphs, nor Document, 
    // since we can always retrieve them by sentences
    private Map<StorageLevel, Storage> storageLevelMap = new HashMap<>(3);

    public DocumentStorage(String dirPath) throws SQLException, IOException {

        Files.createDirectories(Paths.get(dirPath));

        storageLevelMap.put(StorageLevel.DOCUMENT, new Storage(
            dirPath + "documentStorage.data",
            dirPath + "documentStorage.idx", 
            dirPath + "documentStorage.meta"));

        storageLevelMap.put(StorageLevel.PARAGRAPH, new Storage(
            dirPath + "paragraphStorage.data", 
            dirPath + "paragraphStorage.idx", 
            dirPath + "paragraphStorage.meta"));

        storageLevelMap.put(StorageLevel.SENTENCE, new Storage(
            dirPath + "sentenceStorage.data", 
            dirPath + "sentenceStorage.idx", 
            dirPath + "sentenceStorage.meta"));

        dbInitializator = new DBInitializator(dirPath);
    }

    /**
     * This function guarantees that some document will be saved
     * new document, new url -> Everything will get saved
     * new document, old url -> only document and new occurrence
     * old docuemnt, new url -> only url and new occurrence
     * old docuemnt, old url -> only new occurrence
     * @param bytes
     * @param indexerId
     * @param path path or url
     * @param meta
     * @param contentType
     * @param lastChangeTime
     * @param clock
     * @throws IOException
     * @throws SQLException
     * @throws RecordAlreadyExists
     * @throws RecordsFileException
     */
    public DocumentRecord saveDocument(byte[] bytes, String indexerId, String path, String meta,
            String contentType, long lastChangeTime, boolean saveBinary, Clock clock)
            throws SQLException, IOException {

        DocumentRecord documentRecord = null;
        for (int retries = 0; retries < 5; retries++) {
            Connection conn = dbInitializator.getConnection();
            String checksum = RecordChecksum.getChecksum(bytes);

            Url urlRecord = null;

            try {
                synchronized (this) {
                    documentRecord = DocumentRecord.getByHash(conn, checksum);
                }
            } catch (RecordNotFoundException e) {
                System.out.println("Document not found, creating a new one");
            } catch (SQLException e) {
                if (e.getErrorCode() == 5) {
                    System.out.println("Document retrieval Error: BUSY " + path + ", retrying " + retries);
                    continue;
                }
            }

            try {
                synchronized (this) {
                    urlRecord = Url.getByUrl(conn, path);
                }
            } catch (RecordNotFoundException e) {
                System.out.println("Url not found, creating a new one");
            } catch (SQLException e) {
                if (e.getErrorCode() == 5) {
                    System.out.println("URL retrieval Error: BUSY " + path + ", retrying " + retries);
                    continue;
                }
            }

            if (documentRecord == null) {
                try {
                    synchronized (this) {
                        documentRecord = DocumentRecord.create(conn, checksum);
                    }
                    if (saveBinary) {
                        storageLevelMap.get(StorageLevel.DOCUMENT).save(bytes, meta, indexerId, contentType, lastChangeTime, path,
                                documentRecord.getId(), conn);
                    }
                } catch (RecordAlreadyExistsException e) {
                    System.out.println("Database is not synchronized, retrying.");
                    continue;
                } catch (SQLException e) {
                    if (e.getErrorCode() == 5) {
                        System.out.println("Document creation Error: BUSY " + path + ", retrying " + retries);
                        continue;
                    }
                    System.out.println("Error: " + path + " " + retries);
                    System.out.println("Failed to create a document record.");
                    e.printStackTrace();
                    conn.close();
                    throw e;
                }
            }

            if (urlRecord == null) {
                try {
                    synchronized (this) {
                        urlRecord = Url.create(conn, path);
                    }
                } catch (RecordAlreadyExistsException e) {
                    System.out.println("Database is not synchronized, retrying.");
                    continue;
                } catch (SQLException e) {
                    System.out.println("Error: " + path + " " + retries);
                    System.out.println("URL Creation  Failed to create a URL record.");
                    e.printStackTrace();
                    conn.close();
                    throw e;
                }
            }

            try {
                synchronized (this) {
                    OccurrenceRecord occurrence = OccurrenceRecord.create(conn, urlRecord, documentRecord.getId(), indexerId, clock);
                    documentRecord.addOccurrence(occurrence);
                }
            } catch (SQLException e) {
                if (e.getErrorCode() == 5) {
                    System.out.println("Occurance creation Error: BUSY " + path + ", retrying " + retries);
                    continue;
                }
                System.out.println("Failed to create a occurrence record.");
                e.printStackTrace();
                conn.close();
                throw e;
            }

            conn.close();
            break;
        }

        return documentRecord;
    }

    /** Method for batch saving of paragraphs.
     *  Computes hashes for the paragraph batch and saves the batch into Paragraph and DocumentofParagraph tables.
     */
    public ParagraphRecord[] saveParagraphs(Connection conn, String[] paragraphs, DocumentMeta doc) throws SQLException, IOException {

        String[] paragraphHashes = new String[paragraphs.length];
        for (int i = 0; i < paragraphHashes.length; i++) {
            paragraphHashes[i] = RecordChecksum.getChecksum(paragraphs[i]);
        }
        
        ParagraphRecord[] paragraphRecords;
        synchronized (this) {
            paragraphRecords = ParagraphRecord.createMany(conn, paragraphHashes);
            DocumentOfParagraph.createMany(conn, doc.id, paragraphRecords);
        }

        return paragraphRecords;
    }

    public void saveBinary(Connection conn, int id, StorageLevel level, String data, DocumentMeta doc) throws SQLException, IOException {
        storageLevelMap.get(level).save(data.getBytes(), doc.meta, doc.indexerId, doc.contentType,
                doc.lastChangeTime, doc.path, id, conn);
    }

    /** Method for batched saving of sentences. */
    public List<List<SentenceRecord>> saveSentences(Connection conn, List<List<String>> sentencesOfParagraphs, DocumentMeta doc,
            ParagraphRecord[] paragraphRecords) throws SQLException, IOException {

        List<List<String>> sentenceHashes = new LinkedList<>();
        for (List<String> sentences : sentencesOfParagraphs) {
            List<String> hashes = new ArrayList<>();
            for (String sentence : sentences) {
                hashes.add(RecordChecksum.getChecksum(sentence));
            }
            sentenceHashes.add(hashes);
        }

        List<List<SentenceRecord>> sentenceRecords;
        synchronized (this) {
            sentenceRecords = SentenceRecord.createMany(conn, sentenceHashes, paragraphRecords);
            ParagraphOfSentence.createMany(conn, paragraphRecords, sentenceRecords);
            SentenceOccurrence.createMany(conn, doc, paragraphRecords, sentenceRecords);
        }

        return sentenceRecords;
    }

    /**
     * Function for obtaining the DocumentStorage metadata in a map.
     * @return a map with metadata in a format Map[DocID, Map[MetaKey, MetaValue]]
     */
    public Map<String, Map<String, String>> getMeta(StorageLevel level) {
        try {
            return storageLevelMap.get(level).getMetaController().getAllMeta();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Function for obtaining the document record with its indexing information.
     * @param hash - hash of the document.
     * @return - document record.
     * @throws RecordNotFoundException
     * @throws DatabaseException.RecordNotFound
     */
    public DocumentRecord getDocumentRecord(String hash) throws RecordNotFoundException {
        DocumentRecord doc;

        try {
            Connection conn = dbInitializator.getConnection();
            doc = DocumentRecord.getByHash(conn, hash);
            conn.close();
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }

        return doc;
    }

    /**
     * Function for obtaining the document record by url in a timespan
     * @param url
     * @param lowTime
     * @param highTime
     * @return Document Record
     * @throws RecordNotFoundException
     * @throws DatabaseException.RecordNotFound
     */
    public DocumentRecord getDocumentRecord(String url, long lowTime, long highTime) throws RecordNotFoundException {
        DocumentRecord doc;

        try {
            Connection conn = dbInitializator.getConnection();
            doc = DocumentRecord.getByUrlAndTime(conn, url, lowTime, highTime);
            conn.close();
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }

        return doc;
    }

    /**
     * Function for obtaining the document record by url and approximate timestamp
     * @param url
     * @param timestamp
     * @return Document Record
     * @throws RecordNotFoundException
     * @throws DatabaseException.RecordNotFound
     */
    public DocumentRecord getDocumentRecord(String url, long timestamp) throws RecordNotFoundException {
        DocumentRecord doc;

        try {
            Connection conn = dbInitializator.getConnection();
            doc = DocumentRecord.getClosestByUrl(conn, url, timestamp);
            conn.close();
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }

        return doc;
    }

    /**
     * Function for obtaining the document record by url at or before certain timestamp
     * @param url
     * @param before
     * @return Document Record
     * @throws RecordNotFoundException
     * @throws DatabaseException.RecordNotFound
     */
    public DocumentRecord getDocumentRecordBefore(String url, long before) throws RecordNotFoundException {
        DocumentRecord doc;

        try {
            Connection conn = dbInitializator.getConnection();
            doc = DocumentRecord.getClosestBefore(conn, url, before);
            conn.close();
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }

        return doc;
    }

    /**
     * Function for obtaining the document record by url at or after certain timestamp
     * @param url
     * @param timestamp
     * @return Document Record
     * @throws RecordNotFoundException
     * @throws DatabaseException.RecordNotFound
     */
    public DocumentRecord getDocumentRecordAfter(String url, long before) throws RecordNotFoundException {
        DocumentRecord doc;

        try {
            Connection conn = dbInitializator.getConnection();
            doc = DocumentRecord.getClosestAfter(conn, url, before);
            conn.close();
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }

        return doc;
    }

    /**
     * Obtains record with its indexing information.
     * @param level required level
     * @param hash hash of the record.
     * @return record.
     * @throws RecordNotFoundException
     */
    public AbstractRecord getRecord(StorageLevel level, String hash) throws RecordNotFoundException {
        AbstractRecord record; 

        try {
            Connection conn = dbInitializator.getConnection();
            switch (level) {
            case SENTENCE:
                record = SentenceRecord.getByHash(conn, hash);
                break;
            case PARAGRAPH:
                record = ParagraphRecord.getByHash(conn, hash);
                break;
            default:
                record = DocumentRecord.getByHash(conn, hash);
                break;
            }
            conn.close();
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }

        return record;
    }

    /**
     * Obtains record with its indexing information.
     * @param level required level
     * @param id id of the record.
     * @return record.
     * @throws RecordNotFoundException
     */
    public AbstractRecord getRecord(StorageLevel level, int id) throws RecordNotFoundException {
        AbstractRecord record; 

        try {
            Connection conn = dbInitializator.getConnection();
            switch (level) {
            case SENTENCE:
                record = SentenceRecord.getById(conn, id);
                break;
            case PARAGRAPH:
                record = ParagraphRecord.getById(conn, id);
                break;
            default:
                record = DocumentRecord.getById(conn, id);
                break;
            }
            conn.close();
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }

        return record;
    }

    /**
     * Function for retrieving a document from the document storage with a hash.
     * @param hash - a hash of the document to retrieve.
     * @return - document in the binary form.
     * @throws IOException
     * @throws RecordNotFoundException
     */
    public byte[] getBinaryRecord(StorageLevel level, String hash) throws IOException, RecordNotFoundException {
        AbstractRecord doc = getRecord(level, hash);
        long id = doc.getId();
        long offset = storageLevelMap.get(level).getIndexController().getOffset(id);
        long length = storageLevelMap.get(level).getIndexController().getLength(id);
        return storageLevelMap.get(level).getStorageController().read(offset, length);
    }

    public List<Map<String, String>> getMappedLevels(String[] inValues, String inType,
            String[] outType, MapLevel inLevel, MapLevel outLevel, Optional<MapLevel> overLevel, boolean includeOrigin) {
        List<Map<String, String>> result = new LinkedList<>();
        if (overLevel == null) {
            overLevel = Optional.empty();
        }

        try {
            Connection conn = dbInitializator.getConnection();
            if (overLevel.isEmpty()) {
                result = DocumentMapper.executeNonOver(conn, inValues,
                        inType, outType, inLevel, outLevel, includeOrigin);
            } else {
                result = DocumentMapper.executeOver(conn, inValues,
                        inType, outType, inLevel, outLevel, overLevel.get(), includeOrigin);
            }
            conn.close();
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }

        return result;
    }
}