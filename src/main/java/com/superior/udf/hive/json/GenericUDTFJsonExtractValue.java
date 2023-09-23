package com.superior.udf.hive.json;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.math.BigDecimal;
import java.util.ArrayList;

/**
 * 从json 字符串中获取 key值
 * <p/>
 * <p>For example,
 * <p/>
 * select json_extract_value('{"name":"melin", "address":{"name": "hangzhou"}}', "name", "address/name")
 * as (name, address) from users'
 */
public class GenericUDTFJsonExtractValue extends GenericUDTF {

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2) {
            throw new UDFArgumentLengthException("JsonExtractValueUDTF 至少两个参数");
        }
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("JsonExtractValueUDTF 第一个参数为String类型");
        }

        ArrayList<String>          fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs   = new ArrayList<ObjectInspector>();
        for (int i = 1; i < arguments.length; i++) {
            fieldNames.add("col" + i);
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] arguments) throws HiveException {
        String inputJson = "";
        String[] result = new String[arguments.length - 1];
        try {
            if (arguments[0] instanceof Text) {
                inputJson = ((Text) arguments[0]).toString();
            } else {
                inputJson = (String) arguments[0];
            }

            if (StringUtils.isNotBlank(inputJson)) {
                JsonNode node = mapper.readTree(inputJson);
                for (int i = 1; i < arguments.length; i++) {
                    String key = ((Text) arguments[i]).toString();

                    if (!StringUtils.startsWith(key, "/")) {
                        key = "/" + key;
                    }
                    JsonNode jsonNode = node.at(key);
                    String value;

                    if (jsonNode instanceof MissingNode) {
                        value = null;
                    } else {
                        if (jsonNode.isValueNode()) {
                            if (jsonNode.isBigDecimal()) {
                                BigDecimal decimalValue = jsonNode.decimalValue();
                                value = decimalValue.toPlainString();
                            } else {
                                value = jsonNode.asText();
                            }
                        } else {
                            value = mapper.writeValueAsString(jsonNode);
                        }
                    }

                    result[i - 1] = value;
                }
            }
        } catch (Exception e) {
            result[0] = "error: " + e.getMessage();
        }
        forward(result);
    }

    @Override
    public void close() throws HiveException {
    }
}
