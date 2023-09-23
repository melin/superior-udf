package com.superior.udf.starrocks;

public class HelloUDF {

    public final String evaluate(String name) {
        if (name == null) return null;

        return "Hello " + name;
    }
}
