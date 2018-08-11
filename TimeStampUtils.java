/**
 * 
 */
package com.am.analytics.job.common.utils;

import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.am.analytics.common.core.utils.DataType;
import com.am.analytics.common.core.utils.StringUtils;

/**
 * The Class TimeStampUtils.
 *
 * @author am
 */
public final class TimeStampUtils {

    /**
     * Instantiates a new time stamp utils.
     */
    private TimeStampUtils() {
        // private constructor for util class.
    }

    /**
     * Gets the generic time stamp.
     *
     * @param timeStampDate the time stamp date
     * @return the generic time stamp
     */
    public static Date getGenericTimeStamp(final String timeStampDate) {

        final String[] differentDateFormat = DataType.GENERICTIMESTAMP.getType().split("##");
        Date date = null;
        // date format which contain month name in dateStr has been put at last
        // position in enum
        if (!StringUtils.isEmpty(timeStampDate)) {
            DateFormat dataFormatter = null;
            for (int index = 0; index < differentDateFormat.length; index++) {
                dataFormatter = new SimpleDateFormat(differentDateFormat[index]);
                try {
                    date = new Date(dataFormatter.parse(timeStampDate).getTime());
                    break;
                } catch (final Exception e) {
                    // try new Formatter
                }
            }
        }
        return date;
    }
}
