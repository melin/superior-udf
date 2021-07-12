package com.dataworker.udf.text;

import com.google.common.io.BaseEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

/**
 * Created by melin on 2019-01-11
 */
public class GenericUDFGzipDeCompressBase64Str extends UDF {

    public String evaluate(String content) throws IOException {
        content = StringUtils.replace(content, "\n", "");
        byte[] data = BaseEncoding.base64().decode(content);
        return decompress(data);
    }

    private String decompress(byte[] compressed) throws IOException {
        StringBuilder sb = new StringBuilder();

        try (ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
             GZIPInputStream gis = new GZIPInputStream(bis);
             BufferedReader br = new BufferedReader(new InputStreamReader(gis, "UTF-8"));
             ) {

            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }
}
