package com.dataworker.udf.text;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectShortHashMap;
import gnu.trove.map.hash.TShortObjectHashMap;
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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by melin on 2018/4/16 下午3:32
 */
public class GenericUDTFPhone extends GenericUDTF {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericUDTFPhone.class);

    private static final TIntObjectHashMap phoneInfoMap = new TIntObjectHashMap();

    private static short provinceIndex = 1;

    private static final TObjectShortHashMap provinceMap = new TObjectShortHashMap();

    private static final TShortObjectHashMap provinceMapIndex = new TShortObjectHashMap();

    private static int cityIndex = 1;

    private static final TObjectIntHashMap cityMap = new TObjectIntHashMap();

    private static final TIntObjectHashMap cityMapIndex = new TIntObjectHashMap();

    private static short ispIndex = 1;

    private static final TObjectShortHashMap ispMap = new TObjectShortHashMap();

    private static final TShortObjectHashMap ispMapIndex = new TShortObjectHashMap();

    private static final AtomicBoolean inited = new AtomicBoolean(false);

    private static final AtomicBoolean finished = new AtomicBoolean(false);

    @Override
    public StructObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2) {
            throw new UDFArgumentLengthException("IpGeoUDTF 至少两个参数");
        }
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("IpGeoUDTF 第一个参数为String类型");
        }

        if (inited.compareAndSet(false, true)) {
            long count = 0;

            String hdfsPath = "hdfs://xxx/user/dataworker/files/phone.txt";
            Path path = new Path(hdfsPath);

            InputStream inputStream = null;
            BufferedReader br = null;
            try {
                FileSystem fileSystem = path.getFileSystem(new HiveConf());
                inputStream = fileSystem.open(path);
                br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] items = StringUtils.split(line, '$');
                    if (NumberUtils.isNumber(items[0])) {
                        Object[] result = new Object[3];
                        int phone = Integer.parseInt(items[0]);

                        if (!"_".equals(items[1])) {
                            String province = items[1];
                            short index = provinceMap.get(province);
                            if (index == 0) {
                                index = provinceIndex++;
                                provinceMap.put(province, index);
                                provinceMapIndex.put(index, province);
                            }
                            result[0] = index;
                        }

                        if (!"_".equals(items[2])) {
                            String city = items[2];
                            int index = cityMap.get(city);
                            if (index == 0) {
                                index = cityIndex++;
                                cityMap.put(city, index);
                                cityMapIndex.put(index, city);
                            }
                            result[1] = index;
                        }

                        if (!"_".equals(items[3])) {
                            String isp = items[3];
                            short index = ispMap.get(isp);
                            if (index == 0) {
                                index = ispIndex++;
                                ispMap.put(isp, index);
                                ispMapIndex.put(index, isp);
                            }
                            result[2] = index;
                        }

                        phoneInfoMap.put(phone, result);
                        count++;
                    }
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            } finally {
                IOUtils.closeQuietly(inputStream);
                IOUtils.closeQuietly(br);
            }

            LOGGER.info("加载Phone 数据：" + count);
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
        String phone = "";
        Object[] result = new Object[arguments.length - 1];
        try {
            if (arguments[0] instanceof Text) {
                phone = ((Text) arguments[0]).toString();
            } else {
                phone = (String) arguments[0];
            }
            if(StringUtils.isBlank(phone)) {
                return;
            }

            phone = StringUtils.substring(phone, 0, 7);
            if(!NumberUtils.isNumber(phone)) {
                return;
            }

            int phoneInt = Integer.parseInt(phone);
            Object[] objects = (Object[])phoneInfoMap.get(phoneInt);

            for (int i = 1; i < arguments.length; i++) {
                String key = arguments[i].toString();
                Object obj = null;
                switch (key) {
                    case "province":
                        obj = objects[0];
                        if (obj != null) {
                            result[i - 1] = provinceMapIndex.get((short) obj);
                        }
                        break;
                    case "city":
                        obj = objects[1];
                        if (obj != null) {
                            result[i - 1] = cityMapIndex.get((int) obj);
                        }
                        break;
                    case "isp":
                        obj = objects[2];
                        if (obj != null) {
                            result[i - 1] = ispMapIndex.get((short) obj);
                        }
                        break;
                    default:
                        result[i - 1] = null;
                        break;
                }
            }

        } catch (Exception e) {
            result[0] = "parse error: " + e.getMessage();
        }
        forward(result);
    }

    @Override
    public void close() throws HiveException {
        /*phoneInfoMap.clear();
        provinceMap.clear();
        provinceMapIndex.clear();
        cityMap.clear();
        cityMapIndex.clear();
        ispMap.clear();
        ispMapIndex.clear();*/
    }
}
