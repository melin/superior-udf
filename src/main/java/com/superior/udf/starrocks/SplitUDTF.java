package com.superior.udf.starrocks;

public class SplitUDTF {
    public String[] process(String in) {
        if (in == null) return null;
        return in.split(" ");
    }
}
