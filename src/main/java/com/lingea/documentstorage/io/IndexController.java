package com.lingea.documentstorage.io;

import java.io.IOException;

/**
 * Class for manipulating the document storage index.
 */
public class IndexController extends AbstractController {
    private long LENGTH_OF_LONG = 8;

    public IndexController(String filename) throws IOException {
        super(filename);
    }

    /**
     * Get the byte offset of the position-th document in the index.
     * @param documentId - id of the document.
     * @return - byte offset of the document metadata.
     * @throws IOException
     */
    public long getOffset(long documentId) throws IOException {
        return getMetaFromIndex(computeOffsetPosition(documentId - 1));
    }

    /**
     * Get the byte length of the position-th document in the index.
     * @param documentId- id of the document.
     * @return - byte length of the document metadata.
     * @throws IOException
     */
    public long getLength(long documentId) throws IOException {
        return getMetaFromIndex(computeLengthPosition(documentId - 1));
    }

    /**
     * Function for writing document metadata to the index.
     * @param offset - byte offset of the document in the storage.
     * @param length - byte length of the document in the storage.
     * @throws IOException
     */
    public void writeToIndex(long id, long offset, long length) throws IOException {
        FileUtils.writeBytesToRAF(this.filename, computeOffsetPosition(id - 1), Cast.longToByteArray(offset));
        FileUtils.writeBytesToRAF(this.filename, computeLengthPosition(id - 1), Cast.longToByteArray(length));
    }

    /** Computes byte offset of the document offset in the index. */
    public long computeOffsetPosition(long position) {
        return position * LENGTH_OF_LONG * 2;
    }

    /** Computes byte offset of the document length in the index. */
    public long computeLengthPosition(long position) {
        return computeOffsetPosition(position) + LENGTH_OF_LONG;
    }

    /** Reads metadata from the index. */
    private long getMetaFromIndex(long offset) throws IOException {
        try {
            byte[] result = FileUtils.readBytesFromRAF(this.filename, offset, offset + LENGTH_OF_LONG);
            return Cast.byteArrayToLong(result);
        } catch (IOException ex) {
            throw ex;
        }
    }
}
