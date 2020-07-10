package com.dataworker.udf.json;

import org.apache.commons.lang3.StringUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 输入Key后，剔除JSON中的VALUE，然后返回剩余的JSON
 *
 */
public class GenericUDFJsonExtractRemainValue extends UDF {

    private static final String KEY_SPLIT_CHARS = ",";

    private static final String FIELD_SPLIT_CHARS = "#";

    /***
     *
     * @param json
     * @return
     */
    public String evaluate(String json) {
        return json;
    }

    //从JSON中剔除keyStr中对应的value,返回剩下的JSON,keyStr为key字符串
    //keyStr: "aaa#bbb#ccc,ddd,eee"
    public String evaluate(String json,String keyStr) {
        Object jsonObj = JSON.parse(json);
        String[] keyarr = StringUtils.split(keyStr, KEY_SPLIT_CHARS);

        if (jsonObj instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) jsonObj;
            for (int i = 0; i < keyarr.length; i++) {
                if (keyarr[i].indexOf(FIELD_SPLIT_CHARS) != -1) {
                    removeKey(jsonObject, keyarr[i]);
                } else {
                    jsonObject.remove(keyarr[i]);
                }
            }
            return jsonObject.toJSONString();

        } else {
            return json;
        }
    }

    //从JSON中剔除keyStr中对应的value,返回剩下的JSON,keyStr为key字符串
    //0位置为JSON数组，"aaa","bbb#dd","ccc"为单独的参数
    public String evaluate(Object[] arguments) {
        String obj = (String) arguments[0];
        Object jsonObj = JSON.parse(obj);

        if (jsonObj instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) jsonObj;
            for (int i = 1; i < arguments.length; i++) {
                 //对对象数组进行String转化
                 String its = (String) arguments[i];
                if (its.indexOf(FIELD_SPLIT_CHARS) != -1) {
                    removeKey(jsonObject, its);
                } else {
                    jsonObject.remove(its);
                }
            }
            return jsonObject.toJSONString();

        } else {
            return obj;
        }
    }

    //递归移除解析后的key,比如：aaaa#bbbb，那么移除aaaa域下的bbbb这个key对应的value
    private void removeKey(JSONObject jsonObject,String key) {
        int index = key.indexOf(FIELD_SPLIT_CHARS);
        if (index != -1) {
            String firstKey = key.substring(0, index);
            String restKey = key.substring(index + 1);
            Object childObject = jsonObject.get(firstKey);
            if (childObject instanceof JSONObject) {
                removeKey((JSONObject) childObject, restKey);
            }
        } else {
            jsonObject.remove(key);
        }
    }
}
