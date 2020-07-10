package com.dataworker.udf.number;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.util.concurrent.ThreadLocalRandom;

/**
 * random_int(maxValue) 生成0-maxValue 区间随机整数值，包含maxValue。
 */
@UDFType(deterministic = false)
public class GenericUDFRandomInt extends UDF {

    /**
     * @return
     */
    public int evaluate() {
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        return threadLocalRandom.nextInt();
    }

    /**
     * @param maxValue
     * @return
     */
    public int evaluate(int maxValue) {
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        return threadLocalRandom.nextInt(maxValue);
    }

    /**
     * @param maxValue
     * @return
     */
    public String evaluate(String value, int maxValue) {
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        return value + "-" + threadLocalRandom.nextInt(maxValue);
    }
}
