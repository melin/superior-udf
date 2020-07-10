package org.apache.spark.sql;

/**
 * Created by melin on 2019-06-27
 */
public class DataWorkerUDFException extends RuntimeException {

    public DataWorkerUDFException(String message) {
        super(message);
    }

    public DataWorkerUDFException(String message, Throwable cause) {
        super(message, cause);
    }
}
