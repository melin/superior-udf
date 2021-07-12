package com.dataworker.udf.text;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(StandaloneHiveRunner.class)
public class GenericUDFSha512EncryptTest {

    @HiveSQL(files = {"text/sha512_encrypt.sql"}, autoStart = true)
    public HiveShell hiveShell;

    @Test
    public void testSelect() {
        // 查数据
        List<String> result = hiveShell.executeQuery("select sha512_encrypt('test', 'salt') as text");
        Assert.assertEquals(result.get(0), "07e007fe5f99ee5851dd519bf6163a0d2dda54d45e6fe0127824f5b45a5ec59183a08aaa270979deb2f048815d05066c306e3694473d84d6aca0825c3dccd559");
    }
}
