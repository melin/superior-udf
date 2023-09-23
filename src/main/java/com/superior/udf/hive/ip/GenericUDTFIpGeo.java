package com.superior.udf.hive.ip;

import com.superior.udf.hive.util.InetAddrHelper;
import com.superior.udf.hive.util.LongArrayListUtils;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
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
public class GenericUDTFIpGeo extends GenericUDTF {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericUDTFIpGeo.class);

    private static final TLongObjectHashMap ipInfoMap = new TLongObjectHashMap();

    private static final TLongArrayList ipList = new TLongArrayList();

    private static int countryIndex = 1;

    private static final TObjectIntHashMap countryMap = new TObjectIntHashMap();

    private static final TIntObjectHashMap countryMapIndex = new TIntObjectHashMap();

    private static int provinceIndex = 1;

    private static final TObjectIntHashMap provinceMap = new TObjectIntHashMap();

    private static final TIntObjectHashMap provinceMapIndex = new TIntObjectHashMap();

    private static int cityIndex = 1;

    private static final TObjectIntHashMap cityMap = new TObjectIntHashMap();

    private static final TIntObjectHashMap cityMapIndex = new TIntObjectHashMap();

    private static int ispIndex = 1;

    private static final TObjectIntHashMap ispMap = new TObjectIntHashMap();

    private static final TIntObjectHashMap ispMapIndex = new TIntObjectHashMap();

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

            String hdfsPath = "hdfs://tdhdfs-spark-hz/user/datacompute/users/melin/geoip.txt";
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
                        Object[] result = new Object[6];
                        long ip = Long.parseLong(items[0]);
                        ipList.add(ip);

                        if (!"_".equals(items[1])) {
                            String conutry = items[1];
                            int index = countryMap.get(conutry);
                            if (index == 0) {
                                index = countryIndex++;
                                countryMap.put(conutry, index);
                                countryMapIndex.put(index, conutry);
                            }
                            result[0] = index;
                        }

                        if (!"_".equals(items[2])) {
                            String province = items[2];
                            int index = provinceMap.get(province);
                            if (index == 0) {
                                index = provinceIndex++;
                                provinceMap.put(province, index);
                                provinceMapIndex.put(index, province);
                            }
                            result[1] = index;
                        }

                        if (!"_".equals(items[3])) {
                            String city = items[3];
                            int index = cityMap.get(city);
                            if (index == 0) {
                                index = cityIndex++;
                                cityMap.put(city, index);
                                cityMapIndex.put(index, city);
                            }
                            result[2] = index;
                        }

                        if (!"_".equals(items[4])) {
                            String isp = items[4];
                            int index = ispMap.get(isp);
                            if (index == 0) {
                                index = ispIndex++;
                                ispMap.put(isp, index);
                                ispMapIndex.put(index, isp);
                            }
                            result[3] = index;
                        }

                        if (NumberUtils.isNumber(items[5])) {
                            double latitude = Double.parseDouble(items[5]);
                            result[4] = latitude;
                        }

                        if (NumberUtils.isNumber(items[6])) {
                            double longitude = Double.parseDouble(items[6]);
                            result[5] = longitude;
                        }

                        ipInfoMap.put(ip, result);
                        count++;
                    }
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            } finally {
                IOUtils.closeQuietly(inputStream);
                IOUtils.closeQuietly(br);
            }

            LOGGER.info("加载IP 数据：" + count);
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
        String ip = "";
        Object[] result = new Object[arguments.length - 1];
        try {
            if (arguments[0] instanceof Text) {
                ip = ((Text) arguments[0]).toString();
            } else {
                ip = (String) arguments[0];
            }
            if(StringUtils.isBlank(ip)) {
                return;
            }

            long ipLong = InetAddrHelper.IPToLong(ip);

            if(ipLong > 0) {
                int index = LongArrayListUtils.bisectRight(ipList, ipLong);
                if(index > 0) {
                    long value = ipList.get(index - 1);
                    Object[] objects = (Object[])ipInfoMap.get(value);

                    for (int i = 1; i < arguments.length; i++) {
                        String key = arguments[i].toString();

                        Object obj = null;
                        switch (key) {
                            case "country":
                                obj = objects[0];
                                if(obj != null) {
                                    result[i - 1] = countryMapIndex.get((int)obj);
                                }
                                break;
                            case "province":
                                obj = objects[1];
                                if(obj != null) {
                                    result[i - 1] = provinceMapIndex.get((int)obj);
                                }
                                break;
                            case "city":
                                obj = objects[2];
                                if(obj != null) {
                                    result[i - 1] = cityMapIndex.get((int)obj);
                                }
                                break;
                            case "isp":
                                obj = objects[3];
                                if(obj != null) {
                                    result[i - 1] = ispMapIndex.get((int)obj);
                                }
                                break;
                            case "latitude":
                                result[i - 1] = objects[4];
                                break;
                            case "longitude":
                                result[i - 1] = objects[5];
                                break;
                            default:
                                result[i - 1] = null;
                                break;
                        }
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
