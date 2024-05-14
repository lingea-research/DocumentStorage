package com.lingea.documentstorage.utils;

public enum MapLevel {
    SENTENCE,
    PARAGRAPH,
    DOCUMENT,
    OCCURRENCE,
    URL;

    @Override
    public String toString() {
        String uc = super.toString();
        String lc = uc.substring(1).toLowerCase();
        String out = uc.toCharArray()[0] + lc;
        return out;
    }
}
