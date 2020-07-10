package com.dataworker.udf.str;

import java.security.MessageDigest;
import org.apache.hadoop.hive.ql.exec.UDF;

public class GenericUDFSha512Encrypt extends UDF {

    /**
     *
     * @param msg
     * @param salt
     * @return
     */
    public String evaluate(String msg, String salt) {
        return encryptSHA(msg,salt);
    }

    private String encryptSHA(String msg,String salt) {
        StringBuilder sb = new StringBuilder();
        try{
            MessageDigest md = MessageDigest.getInstance("SHA-512");
            md.update(salt.getBytes());
            byte[] bytes = md.digest(msg.getBytes());
            for(int i=0; i< bytes.length ;i++){
                sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
            }
        } catch(Exception e) {
            new StringBuilder(e.getMessage());
        }

        return sb.toString();
    }

}
