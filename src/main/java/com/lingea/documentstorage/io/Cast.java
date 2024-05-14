package com.lingea.documentstorage.io;

/**
 * Static class with functions for long-byte[] casting.
 */
public class Cast {

    /**
    * Reads a byte array as a long value in big-endian byte order.
    * 
    * @param byteArray The byte array to be read as a long value.
    * @return The long value represented by the byte array.
    */
    public static long byteArrayToLong(byte[] byteArray) {
        long result = 0L;
        for (int i = 0; i < 8; i++) {
            result <<= 8; // Shift the result to the left by 8 bits
            result |= (byteArray[i] & 0xFF); // Bitwise OR with the byte value
        }
        return result;
    }

    /**
    * Converts a long value to a byte array.
    * 
    * @param value The long value to be converted.
    * @return A byte array representing the long value in big-endian byte order.
    */
    public static byte[] longToByteArray(long value) {
        byte[] byteArray = new byte[8]; // Create a byte array of size 8 (for long)
        for (int i = 7; i >= 0; i--) {
            byteArray[i] = (byte) (value & 0xFF); // Extract the least significant byte
            value >>= 8; // Shift the value to the right by 8 bits
        }
        return byteArray;
    }
}
