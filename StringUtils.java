package com.hbase.poc.utils;

public final class StringUtils extends org.apache.commons.lang3.StringUtils {

    private StringUtils() {

    }

    // Concatenate a set of String arguments
    public static String concatenate(final String... strings) {

        String finalString = null;
        if (!ArrayUtils.isEmpty(strings)) {
            final StringBuilder builder = new StringBuilder();
            for (final String arg : strings) {
                if (isNotEmpty(arg)) {
                    builder.append(arg);
                }
            }
            finalString = builder.toString();
        }
        return finalString;
    }

    // Concatenate a set of Object arguments
    public static String concatenate(final Object... objects) {

        String finalString = null;
        if (!ArrayUtils.isEmpty(objects)) {
            final StringBuilder builder = new StringBuilder();
            for (final Object arg : objects) {
                if (null != arg && isNotEmpty(arg.toString())) {
                    builder.append(arg.toString());
                }
            }
            finalString = builder.toString();
        }
        return finalString;
    }

    public static String concatenateUsingSeparator(final String separator, final Object... objects) {

        String finalString = null;
        if (!ArrayUtils.isEmpty(objects)) {
            final StringBuilder builder = new StringBuilder();
            int i = 0;
            final int totalFiles = objects.length;
            for (final Object obj : objects) {
                if (null != obj) {
                    builder.append(obj.toString());
                }
                if (i != totalFiles - 1) {
                    builder.append(separator);
                }
                i++;
            }
            finalString = builder.toString();
        }
        return finalString;
    }

    public static String concatenateUsingSeparator(final String separator, final String... objects) {

        String finalString = null;
        if (!ArrayUtils.isEmpty(objects)) {
            final StringBuilder builder = new StringBuilder();
            int i = 0;
            final int totalFiles = objects.length;
            for (final Object obj : objects) {
                if (null != obj) {
                    builder.append(obj.toString());
                }
                if (i != totalFiles - 1) {
                    builder.append(separator);
                }
                i++;
            }
            finalString = builder.toString();
        }
        return finalString;
    }

    /**
     * This method returns the string in lowercase if it is not string is not null. It returns null if corresponding string is null.
     * 
     * @param string
     * @return
     */
    public static String toLowerCase(final String string) {

        String lowerCaseString = null;
        if (null != string) {
            lowerCaseString = string.toLowerCase();
        }
        return lowerCaseString;
    }

    /**
     * This method returns the toString of object if object is not null. It returns null if corresponding object is null.
     * 
     * @param object
     * @return
     */
    public static String toString(final Object object) {

        String string = null;
        if (null != object) {
            string = object.toString();
        }
        return string;
    }

    public static void main(String[] args) {

        System.out.println(concatenateUsingSeparator("abcd", "1", null, "3").split("abcd").length);
    }
}
