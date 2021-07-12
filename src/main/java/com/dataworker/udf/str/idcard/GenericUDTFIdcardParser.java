package com.dataworker.udf.str.idcard;

import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by melin on 2018/4/13 下午5:35
 */
public class GenericUDTFIdcardParser extends GenericUDTF {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericUDTFIdcardParser.class);

    private static final TIntObjectHashMap countyInfoMap = new TIntObjectHashMap();

    private static final TIntObjectHashMap cityInfoMap = new TIntObjectHashMap();

    private static final AtomicBoolean inited = new AtomicBoolean(false);

    private static final AtomicBoolean finished = new AtomicBoolean(false);

    @Override
    public StructObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2) {
            throw new UDFArgumentLengthException("IdcardParserUDTF 至少两个参数");
        }
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("IdcardParserUDTF 第一个参数为String类型");
        }

        if (inited.compareAndSet(false, true)) {
            long count = 0;

            String hdfsPath = "hdfs://xxx/user/dataworker/users/files/dim_district_cur.txt";
            Path path = new Path(hdfsPath);

            InputStream inputStream = null;
            BufferedReader br = null;
            try {
                //从地区编码表中读取市、县，省份代码在udf.str.idcard
                FileSystem fileSystem = path.getFileSystem(new HiveConf());
                inputStream = fileSystem.open(path);
                br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] items = StringUtils.split(line, ',');
                    if (items.length >= 4 && NumberUtils.isNumber(items[0])) {
                        int code = Integer.parseInt(items[0]);
                        String county = items[3];
                        String city = items[2];
                        //String province = items[1]; --2018.6.13
                        //String county = items[1];
                        //String city = items[2];
                        //String province = items[3];
                        String status = "";
                        if (items.length > 5) {
                            status = items[4];
                        }
                        if (!"rename".equals(status)) {
                            countyInfoMap.put(code, county);
                            cityInfoMap.put(code, city);
                        }
                    }
                    count++;
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            } finally {
                IOUtils.closeQuietly(inputStream);
                IOUtils.closeQuietly(br);
            }

            LOGGER.info("加载地区编码数据总数：" + count);
            finished.getAndSet(true);
        } else {
            while (!finished.get()) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (Exception e) {}
            }
            LOGGER.info("等待加载完成");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs   = new ArrayList<ObjectInspector>();
        for (int i = 1; i < arguments.length; i++) {
            fieldNames.add("col" + i);
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] arguments) throws HiveException {
        String idcard = "";
        Object[] result = new Object[arguments.length - 1];
        try {
            if (arguments[0] instanceof Text) {
                idcard = ((Text) arguments[0]).toString();
            } else {
                idcard = (String) arguments[0];
            }

            if(StringUtils.isNotBlank(idcard)) {
                Map<String, Object> infoMap = IdcardUtils.parserIdCard(idcard);
                int prefixCode = Integer.parseInt(StringUtils.substring(idcard, 0, 6));
                for (int i = 1; i < arguments.length; i++) {
                    String key = arguments[i].toString();
                    if ("county".equals(key)) {
                        result[i - 1] = countyInfoMap.get(prefixCode);
                    } else if("city".equals(key)) {
                        result[i - 1] = cityInfoMap.get(prefixCode);
                    } else {
                        result[i - 1] = infoMap.get(key);
                    }
                }
            }
        } catch (Exception e) {
            result[0] = "parse error: " + e.getMessage();
        }
        forward(result);
    }

    @Override
    public void close() throws HiveException {
    }
}
