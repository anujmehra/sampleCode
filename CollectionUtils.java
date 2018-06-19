package com.aeromexico.analytics.common.core.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * The Class CollectionUtils - Utility class created to handle commonly used Collections related operations..
 */
public class CollectionUtils extends org.springframework.util.CollectionUtils {

    /**
     * Instantiates a new collection utils.
     */
    private CollectionUtils() {

    }

    /**
     * To list.
     *
     * @param <T> the generic type
     * @param iterable the iterable
     * @return the list
     */
    public static <T> List<T> toList(final Iterable<T> iterable) {

        List<T> finalList = null;
        if (iterable instanceof List) {
            finalList = (List<T>) iterable;
        } else {
            if (null != iterable) {
                finalList = new ArrayList<>();
                for (final T element : iterable) {
                    finalList.add(element);
                }
            }
        }
        return finalList;
    }

    /**
     * To list.
     *
     * @param <U> the generic type
     * @param <V> the value type
     * @param <T> the generic type
     * @param map the map
     * @param biFunction the bi function
     * @return the list
     */
    public static <U, V, T> List<T> toList(final Map<U, V> map, final BiFunction<U, V, T> biFunction) {

        List<T> list = null;
        if (null != map) {
            final List<T> finalList = new ArrayList<>();
            map.forEach((key, value) -> {
                final T output = biFunction.apply(key, value);
                finalList.add(output);
            });
            list = finalList;
        }
        return list;
    }

    /**
     * Gets the max occurring value in the collection.
     *
     * @param <T> the generic type
     * @param collection collection from which max occurring value is to be picked.
     * @return T max occurring value, it ignores the null value.
     */
    public static <T> T maxOccuringValue(final Collection<T> collection) {

        int maxCount = 0;
        T maxOccuringValue = null;
        final Map<T, Integer> map = new HashMap<>();
        if (!CollectionUtils.isEmpty(collection)) {
            for (final T element : collection) {
                if (null != element) {
                    Integer count = map.get(element);
                    count = count == null ? 1 : count + 1;
                    map.put(element, count);
                    if (count > maxCount) {
                        maxCount = count;
                        maxOccuringValue = element;
                    }
                }
            }
        }
        return maxOccuringValue;
    }
}
