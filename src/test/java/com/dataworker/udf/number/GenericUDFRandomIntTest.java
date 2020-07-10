package com.dataworker.udf.number;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

@RunWith(StandaloneHiveRunner.class)
public class GenericUDFRandomIntTest {

    @HiveSQL(files = {"number/random.sql"}, autoStart = true)
    public HiveShell hiveShell;

    @Test
    public void testHello() {
        // 查数据
        List<String> result = hiveShell.executeQuery("select randomInt(10) from test_db.hello_world");

        // 检查查询是否成功
        Assert.assertEquals(result.size(), 1);
        Assert.assertTrue(Long.valueOf(result.get(0)) <= 10);
    }
}
