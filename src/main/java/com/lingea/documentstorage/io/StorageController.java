package com.lingea.documentstorage.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Class for reading to and writing from the document storage.
 */
public class StorageController extends AbstractController {

    public StorageController(String filename) throws IOException {
        super(filename);
    }

    /**
     * Returns file size of the document storage, this is usefull
     * for retrieving the offset during write.
     * @return - byte size of document storage.
     * @throws IOException
     */
    public long getSize() throws IOException {
        Path path = Path.of(this.filename);
        if (Files.exists(path)) {
            return Files.size(path);
        }
        return 0;
    }

    /**
     * Function for writing into the document storage.
     * @param data - bytes of the document to store.
     * @throws IOException
     */
    public void write(byte[] data) throws IOException {
        FileUtils.writeBytesToEndOfRAF(this.filename, data);
    }

    /**
     * Function for reading the document from the document storage.
     * Offset and length should be obtained from the IndexController class.
     * @param offset - byte offset of the document in the storage.
     * @param length - byte length of the document in the storage.
     * @return - bytes of the document
     * @throws IOException
     */
    public byte[] read(long offset, long length) throws IOException {
        return FileUtils.readBytesFromRAF(this.filename, offset, offset + length);
    }
}
