package com.dataworker.udf.text;

import org.apache.spark.sql.DataWorkerUDFException;
import org.apache.commons.codec.digest.HmacUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by melin on 2019-06-27
 */
public class GenericUDFHmacEncrypt extends UDF {

    /**
     *
     * @param algorithm
     * @param msg
     * @param salt
     * @return
     */
    public String evaluate(String algorithm, String msg, String salt) {
        if ("md5".equals(algorithm)) {
            return HmacUtils.hmacMd5Hex(salt, msg);
        } else if ("sha1".equals(algorithm)) {
            return HmacUtils.hmacSha1Hex(salt, msg);
        } else if ("sha256".equals(algorithm)) {
            return HmacUtils.hmacSha256Hex(salt, msg);
        } else if ("sha384".equals(algorithm)) {
            return HmacUtils.hmacSha384Hex(salt, msg);
        } else if ("sha512".equals(algorithm)) {
            return HmacUtils.hmacSha512Hex(salt, msg);
        } else {
            throw new DataWorkerUDFException("hmac 不支持的参数");
        }
    }
}
