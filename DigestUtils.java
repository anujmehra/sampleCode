package com.hbase.poc.utils;

import java.io.UnsupportedEncodingException;

/**
 * Digest Utils class to be used when a digest needs to be generated for a key.
 * <p>
 * In current implementation digest is generated using SHA-256 algorithm. This class is to be used while creating a digest for the
 * database key, in case of HBase, to make sure data is inserted randomly acrross different partitions.
 * </p>
 * 
 */
public final class DigestUtils {

    private DigestUtils() {

    }

    /**
     * Generates the hash of the key. In current implementation hash is the SHA-256 haash.
     * 
     * @param key {@link String} key whose hash is to be calculated
     * @return {@link String} the hexadecimal representation of hash for the key.
     */
    public static String generateHash(final String key) {

        String hash = null;
        if (!StringUtils.isEmpty(key)) {
            try {
                final byte[] bytes = key.getBytes("UTF-8");
                hash = generateHash(bytes);
            } catch (final UnsupportedEncodingException e) {
                throw new IllegalArgumentException("Error occured while generating hash for string ", e);
            }
        }
        return hash;
    }

    /**
     * Generates the hash for the bytes in the <code> byte</code> array.
     * 
     * @param bytes <code>byte</code> array that represents data whose hash is to be generated.
     * @return {@link String} hex representation of the generated hash
     */
    public static String generateHash(final byte[] bytes) {

        String hash = null;
        if (!ArrayUtils.isEmpty(bytes)) {
            hash = org.apache.commons.codec.digest.DigestUtils.sha256Hex(bytes);
        }
        return hash;
    }
    
    public static void main(String[] args) throws Exception {
        System.out.println("1be4d51f008103a7060581931a761240".length());
    }
}
