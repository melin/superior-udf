package com.superior.udf.util;

import gnu.trove.list.array.TLongArrayList;

/**
 * Created by binsong.li on 2018/4/17 下午7:21
 */
public class LongArrayListUtils {

    public static int bisectRight(TLongArrayList list, long value) {
        return bisectRight(list, value, 0, list.size());
    }

    public static int bisectRight(TLongArrayList list, long value, int startIndex, int endIndex) {
        int size = list.size();
        if (size == 0) {
            return 0;
        }
        if (value < list.get(startIndex)) {
            return startIndex;
        }
        if (value > list.get(endIndex - 1)) {
            return endIndex;
        }
        for (;;) {
            if (startIndex + 1 == endIndex) {
                return startIndex + 1;
            }
            int mi = (endIndex + startIndex) / 2;
            if (value < list.get(mi)) {
                endIndex = mi;
            } else {
                startIndex = mi;
            }
        }
    }

    public static int bisectLeft(TLongArrayList list, long value) {
        return bisectLeft(list, value, 0, list.size());
    }

    public static int bisectLeft(TLongArrayList list, long value, int startIndex, int endIndex) {
        int size = list.size();
        if (size == 0) {
            return 0;
        }
        if (value < list.get(startIndex)) {
            return startIndex;
        }
        if (value > list.get(endIndex - 1)) {
            return endIndex;
        }
        for (;;) {
            if (startIndex + 1 == endIndex) {
                return value == list.get(startIndex) ? startIndex : (startIndex + 1);
            }
            int mi = (endIndex + startIndex) / 2;
            if (value <= list.get(mi)) {
                endIndex = mi;
            } else {
                startIndex = mi;
            }
        }
    }
}
