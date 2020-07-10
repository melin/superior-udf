package com.dataworker.udf.str;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

/**
 * Created by melin on 2018-08-27
 */
public class GenericUDFMaxDs extends UDF {

	private String partitionSpec = null;

	public String evaluate(String databaseName, String tableName) {
		if (partitionSpec == null) {
			try {
				String url = "http://x.x.x.x/v1/getMaxPartition?databaseName=" + databaseName + "&tableName=" + tableName;
				HttpGet request = new HttpGet(url);
				HttpClient httpClient = new DefaultHttpClient();
				HttpResponse response = httpClient.execute(request);

				if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
					String result = EntityUtils.toString(response.getEntity(), "utf-8");
					JSONObject object = JSON.parseObject(result);

					if (!object.getBoolean("success")) {
						throw new RuntimeException("表不存在，或者不是分区表");
					} else {
						partitionSpec = object.getString("data");
						String[] parts = StringUtils.split(partitionSpec, ",");
						//取一级分区值
						if (parts != null && parts.length > 0) {
							partitionSpec = StringUtils.substringAfter(parts[0], "=");
							partitionSpec = StringUtils.trim(partitionSpec);
						} else {
							partitionSpec = "";
						}
					}
				} else {
					partitionSpec = "";
				}
			} catch (Exception e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		}

		return partitionSpec;
	}
}
