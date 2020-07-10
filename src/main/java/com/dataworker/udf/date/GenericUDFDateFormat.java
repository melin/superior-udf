package com.dataworker.udf.date;

import java.sql.Timestamp;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by libinsong on 2017/6/7.
 */
public class GenericUDFDateFormat extends UDF {

    /**
     *
     * @param dateTime
     * @param fromFormat
     * @param toFormat
     * @return
     */
    public String evaluate(String dateTime, String fromFormat, String toFormat) {
        if (dateTime == null) {
            return null;
        }
        SimpleDateFormat formatter = new SimpleDateFormat(fromFormat);
        try {
            Date date = formatter.parse(dateTime);
            formatter = new SimpleDateFormat(toFormat);
            return formatter.format(date);
        } catch (ParseException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    /**
     *
     * @param dateTime
     * @param format
     * @return
     */
    public String evaluate(Timestamp dateTime, String format) {
        if(dateTime == null) {
            return null;
        }
        SimpleDateFormat formatter = new SimpleDateFormat(format);
        return formatter.format(dateTime.getTime());
    }

    /**
     *
     * @param dateTime
     * @param format
     * @return
     */
    public String evaluate(Long dateTime, String format) {
        if (dateTime == null) {
            return null;
        }
        Date date = new Date(dateTime);
        SimpleDateFormat formatter = new SimpleDateFormat(format);
        return formatter.format(date);
    }

    /**
     *
     * @param dateTime
     * @param format
     * @return
     */
    public String evaluate(String dateTime, String format) {
        if (dateTime == null) {
            return null;
        }
        Date date = new Date(Long.valueOf(dateTime));
        SimpleDateFormat formatter = new SimpleDateFormat(format);
        return formatter.format(date);
    }
}
