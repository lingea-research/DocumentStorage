
package com.lingea.documentstorage.io;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Class for storing the filepaths for the storages.
 */
public class Storage {
    private String dataPath;
    private String indexPath;
    private String metaPath;

    private StorageController storageController;
    private IndexController indexController;
    private MetaController metaController;

    /** Constructor setting the paths. */
    public Storage(String dataPath, String indexPath, String metaPath) {
        this.dataPath = dataPath;
        this.indexPath = indexPath;
        this.metaPath = metaPath;

        try {
            storageController = new StorageController(getDataPath());
            indexController = new IndexController(getIndexPath());
            metaController = new MetaController(getMetaPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** Write the document into storage, index it, and write the metadata. */
    public void save(byte[] bytes, String meta, String indexerId, String contentType,
            long lastChangeTime, String path, long docId, Connection conn) throws SQLException, IOException {
        try {
            synchronized(this) {
                indexController.writeToIndex(docId, storageController.getSize(), bytes.length);
                metaController.writeMeta(Long.toString(docId), meta, indexerId, contentType, Long.toString(lastChangeTime), path);
            }

            storageController.write(bytes);

        } catch (Exception ex) {
            conn.close();
            throw ex;
        }
    }

    public String getDataPath() {
        return this.dataPath;
    }

    public String getIndexPath() {
        return this.indexPath;
    }

    public String getMetaPath() {
        return this.metaPath;
    }

    public StorageController getStorageController() {
        return this.storageController;
    }

    public IndexController getIndexController() {
        return this.indexController;
    }

    public MetaController getMetaController() {
        return this.metaController;
    }
}
