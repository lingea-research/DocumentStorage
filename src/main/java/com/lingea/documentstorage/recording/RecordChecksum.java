package com.lingea.documentstorage.recording;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class RecordChecksum {

    public static String getChecksum(String text) {
        return RecordChecksum.getChecksum(text.getBytes());
    }

    public static String getChecksum(Object obj) {
        return RecordChecksum.getChecksum(obj.toString());
    }

    public static String getChecksum(byte[] msg) {
        byte[] hash = null;

        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            hash = md.digest(msg);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        StringBuilder strBuilder = new StringBuilder();
        for (byte b : hash) {
            strBuilder.append(String.format("%02x", b));
        }

        return strBuilder.toString();
    }
}
