package com.dataworker.udf.str;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by melin on 2019/6/19 下午8:52
 */
public class GenericUDFReplaceBetween extends UDF {

    public String evaluate(String input,
                           String start, String end,
                           String replaceWith) {
        return evaluate(input, start, end, false, false, replaceWith);
    }

    public String evaluate(String str,
                           String start,
                           String end,
                           boolean startInclusive,
                           boolean endInclusive,
                           String replaceWith) {

        int i = str.indexOf(start);
        while (i != -1) {
            int j = str.indexOf(end, i + 1);
            if (j != -1) {
                String data = (startInclusive ? str.substring(0, i) : str.substring(0, i + start.length())) +
                        replaceWith;
                String temp = (endInclusive ? str.substring(j + end.length()) : str.substring(j));
                data += temp;
                str = data;
                i = str.indexOf(start, i + replaceWith.length());
            } else {
                break;
            }
        }
        return str;
    }
}
