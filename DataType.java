package com.aeromexico.analytics.common.core.utils;

/**
 * This enum contains the data types used in .yml files.
 */
public enum DataType {

    /** The string. */
    STRING("STRING"),

    /** The integer. */
    INTEGER("INTEGER"),

    /** The real number. */
    REAL_NUMBER("REAL_NUMBER"),

    /** The long. */
    LONG("LONG"),

    /** The date. */
    DATE("DATE"),

    /** The timestamp. */
    TIMESTAMP("yyyy-MM-dd hh:mm:ss"),

    /** The dd mm yyyy. */
    DD_MM_YYYY("dd-MM-yyyy"),

    /** The mm dd yy. */
    MM_DD_YY("MM/dd/yy"),

    /** The yyyy mm dd. */
    YYYY_MM_DD("yyyy-MM-dd"),

    /** The yyyy mm dd hh mm ss. */
    YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd hh:mm:ss"),

    /** The yyyy mm dd hh mm ss s. */
    YYYY_MM_DD_HH_MM_SS_S("yyyy-MM-dd hh:mm:ss"),

    /** The mm dd yyyy. */
    MM_DD_YYYY("MM/dd/yyyy"),

    /** The hh mm ss mm. */
    HH_MM_SS_MM("STRING"),

    /**
     * The generictimestamp. This enum used for timestamp where single column have multiple time stamp formate
     */
    GENERICTIMESTAMP("MM/dd/yyyy hh:mm:ss##yyyy-MM-dd hh:mm:ss##dd-MM-yy hh:mm"),

    /**
     * The genericdate. This enum used for timestamp where single column have multiple date format
     */
    GENERICDATE("dd/MM/yyyy##MM/dd/yyyy##dd-MM-yyyy##MM-dd-yyyy##yyyy-MM-dd##dd-MM-yy##dd-MMMM-yy"),

    /**
     * The dobacspaxdocx. this enum is used for acs pax date formating
     */
    DOBACSPAXDOCX("ddMMMyy##ddMMMyyyy"),

    /**
     * The scientific number. convert scientific number to long
     */
    SCIENTIFIC_NUMBER("LONG"),

    /**
     * The scientific string. convert scientific number to long to string if not possible take as string
     */
    SCIENTIFIC_STRING("STRING"),

    /**
     * The email. check for valid email address
     */
    EMAIL("STRING"),

    /**
     * The real email. check for valid email address and not have invalid keywords
     */
    REAL_EMAIL("STRING");

    /** The type. */
    private final String type;

    /**
     * Instantiates a new data type.
     *
     * @param type the type
     */
    private DataType(final String type) {
        this.type = type;
    }

    /**
     * Gets the type.
     *
     * @return the type
     */
    public String getType() {

        return type;
    }
}
