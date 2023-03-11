package com.superior.udf.json;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

/**
 * Created by toruneko on 2017/8/4.
 */
@RunWith(StandaloneHiveRunner.class)
public class JsonExtractArrayValueTest {

    @HiveSQL(files = {"json/json_extract_array_value.sql"}, autoStart = true)
    public HiveShell hiveShell;

    @Test
    public void testExtractValue() {
        // 查数据
        String json = "[{\"name\":\"zhangsan\", \"age\": 18}, {\"name\":\"wangwu\", \"age\": 18}]";
        List<String> result = hiveShell.executeQuery("select 'hangzhou' as city, b.* from test_db.hello_world a " +
                "lateral view json_extract_array_value('" + json + "', 'name', 'age') b as name, age");

        // 检查查询是否成功
        Assert.assertEquals(result.size(), 2);
    }
}
