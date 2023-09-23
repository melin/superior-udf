package com.superior.udf.hive.json;

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
public class JsonExtractValueTest {

    @HiveSQL(files = {"json/json_extract_value.sql"}, autoStart = true)
    public HiveShell hiveShell;

    @Test
    public void testExtractValue() {
        // 查数据
        List<String> result = hiveShell.executeQuery("select json_extract_value('{\"name\":\"melin\", \"address\":{\"name\": \"hangzhou\"}}', \"name\", \"age\", \"address/name\") as (name, age, address)");

        // 检查查询是否成功
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0), "melin\tNULL\thangzhou");
    }
}
