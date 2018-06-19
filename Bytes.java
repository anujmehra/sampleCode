package com.hbase.poc.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;


public final class Bytes extends org.apache.hadoop.hbase.util.Bytes {

    public static byte[] toBytes(final Serializable serializble) throws IOException{

        byte[] array = null;
        if (null != serializble) {
            try {
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(bos);) {
                    oos.writeObject(serializble);
                    array = bos.toByteArray();
                }
            } catch (final IOException e) {
                throw e;
            }
        }
        return array;
    }

    public static <T> T toObject(final byte[] array) throws IOException,ClassNotFoundException{

        Object object = null;
        if (null != array) {
            try {
                try (ByteArrayInputStream bis = new ByteArrayInputStream(array); ObjectInputStream ois = new ObjectInputStream(bis);) {
                    object = ois.readObject();
                }
            } catch (final IOException e) {
                throw e;
            } catch (final ClassNotFoundException e) {
                throw e;
            }
        }
        return object == null ? null : (T) object;
    }
}
