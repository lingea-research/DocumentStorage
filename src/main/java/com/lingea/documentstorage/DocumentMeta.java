package com.lingea.documentstorage;

public class DocumentMeta {
    public String indexerId;
    public String path;
    public String meta;
    public String contentType;
    public long lastChangeTime;
    public String language;
    public int id;

    public DocumentMeta(String indexerId, String path, String meta, String contentType, long lastChangeTime,
            String language, int id) {
        this.indexerId = indexerId;
        this.path = path;
        this.meta = meta;
        this.contentType = contentType;
        this.lastChangeTime = lastChangeTime;
        this.language = language;
        this.id = id;
    }
}
