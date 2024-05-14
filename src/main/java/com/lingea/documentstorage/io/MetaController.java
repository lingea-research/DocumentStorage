package com.lingea.documentstorage.io;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/** Class for reading and writing document storage metadata. */
public class MetaController extends AbstractController {

    public MetaController(String filename) throws IOException {
        super(filename);
    }

    /** Function for writing indexing metadata to a file. */
    public void writeMeta(String id, String meta, String indexer, String contentType, String lastChangeTime, String url)
            throws IOException {
        String metaString = String
                .format("%s\t%s\t%s\t%s\t%s\t%s",
                        id, meta, indexer, contentType, lastChangeTime, url);
        FileUtils.appendToFile(this.filename, metaString);
    }

    /** Function for getting all metadata from the .meta file. */
    public Map<String, Map<String, String>> getAllMeta() throws IOException {
        List<String> lines = FileUtils.readLinesFromFile(filename);
        Map<String, Map<String, String>> result = new HashMap<>();
        for (String line : lines) {
            String[] fields = line.split("\t");
            Map<String, String> meta = new HashMap<>();
            meta.put("meta", fields[1]);
            meta.put("indexer", fields[2]);
            meta.put("contentType", fields[3]);
            meta.put("lastChangeTime", fields[4]);
            meta.put("url", fields[5]);
            result.put(fields[0], meta);
        }
        return result;
    }
}