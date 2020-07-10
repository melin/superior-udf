package com.dataworker.udf.json;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

/**
 * Created by melin on 2019/6/19 下午8:52
 */
public class GenericUDFGetJsonKeys extends GenericUDF {

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null) {
            return new ArrayList<>();
        }

        Object obj = arguments[0].get();
        String jsonString = null;
        if(obj instanceof String) {
            jsonString = (String) obj;
        } else if(obj instanceof Text) {
            jsonString = ((Text)obj).toString();
        }

        if (StringUtils.isBlank(jsonString)) {
            return new ArrayList<>();
        }
        return Lists.newArrayList(JSON.parseObject(jsonString).keySet());
    }

    @Override
    public String getDisplayString(String[] args) {
        return "get_json_keys(" + args[0] + ")";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("_FUNC_ expects only 1 argument.");
        }

        return ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }
}
