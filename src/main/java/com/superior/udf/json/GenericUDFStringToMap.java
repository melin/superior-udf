package com.superior.udf.json;

import java.util.LinkedHashMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;

/**
 * Created by melin on 2019-07-26 17:06
 */
@Description(name = "json_to_map", value = "_FUNC_(text) - "
        + "Creates a map by parsing json text ")
public class GenericUDFStringToMap extends GenericUDF {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private transient Converter soi_text;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments[0].getCategory() != Category.PRIMITIVE
                || PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
                ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory())
                != PrimitiveGrouping.STRING_GROUP) {
            throw new UDFArgumentException("All argument should be string/character type");
        }

        soi_text = ObjectInspectorConverters.getConverter(arguments[0],
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        LinkedHashMap<String, Object> ret = new LinkedHashMap<String, Object>();

        String text = (String) soi_text.convert(arguments[0].get());
        if (text == null) {
            return ret;
        }

        try {
            ret = objectMapper.readValue(text, new TypeReference<LinkedHashMap<String, Object>>() {
            });
        } catch (Exception e) {
            throw new HiveException("解析json 失败: " + text, e);
        }
        return ret;
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 1);
        return getStandardDisplayString("json_to_map", children, ",");
    }
}
