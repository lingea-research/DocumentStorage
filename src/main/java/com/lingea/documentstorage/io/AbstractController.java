package com.lingea.documentstorage.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

abstract class AbstractController {
    protected String filename;

    /**
     * Basic constructor.
     * @param filename - path to the document storage index.
     * @throws IOException
     */
    public AbstractController(String filename) throws IOException {
        this.filename = filename;

        Path path = Path.of(filename);
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
    }
}
