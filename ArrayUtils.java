package com.hbase.poc.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ArrayUtils extends org.apache.commons.lang3.ArrayUtils {

    private ArrayUtils() {

    }

    public static <T> Set<T> toSet(T[] array) {

        Set<T> set = null;
        if (null != array) {
            set = new HashSet<>();
            for (T element : array) {
                set.add(element);
            }
        }
        return set;
    }
    
    public static <T> List<T> toList(T[] array) {

        List<T> list = null;
        if (null != array) {
            list = new ArrayList<>();
            for (T element : array) {
                list.add(element);
            }
        }
        return list;
    }
}
