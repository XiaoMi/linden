// Copyright 2016 Xiaomi, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.xiaomi.linden.demo.cars;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

// This is a http api demo, not a necessary test case
public class HttpAPIDemo {

  public static String doGet(String strUrl) throws IOException {
    URL url = new URL(strUrl);
    HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
    try {
      httpURLConnection.setConnectTimeout(3000);
      httpURLConnection.setReadTimeout(3000);
      httpURLConnection.setRequestMethod("GET");
      int responseCode = httpURLConnection.getResponseCode();
      if (responseCode == 200) {
        InputStream inputStream = httpURLConnection.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder builder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          builder.append(line);
        }
        inputStream.close();
        return builder.toString();
      }
      throw new IOException("Response code is " + responseCode);
    } finally {
      httpURLConnection.disconnect();
    }
  }

  public static String doPost(String strUrl, String key, String value) throws IOException {
    URL url = new URL(strUrl);
    HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
    try {
      httpURLConnection.setConnectTimeout(3000);
      httpURLConnection.setReadTimeout(3000);
      httpURLConnection.setRequestMethod("POST");
      httpURLConnection.setDoOutput(true);

      StringBuffer params = new StringBuffer();
      params.append(key).append("=").append(value);
      byte[] bytes = params.toString().getBytes();
      OutputStream outputStream = httpURLConnection.getOutputStream();
      outputStream.write(bytes);
      outputStream.flush();
      outputStream.close();
      int responseCode = httpURLConnection.getResponseCode();
      if (responseCode == 200) {
        InputStream inputStream = httpURLConnection.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder builder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          builder.append(line);
        }
        inputStream.close();
        return builder.toString();
      }
      throw new IOException("Response code is " + responseCode);
    } finally {
      httpURLConnection.disconnect();
    }
  }

  // Please set working dir to linden/demo/cars
  public HttpAPIDemo() throws Exception {
    String target = new File(this.getClass().getResource("/").getPath()).getParent();
    Process p = Runtime.getRuntime().exec("sh " + target + "/../bin/cars.sh");
    Assert.assertEquals(0, p.waitFor());
    Thread.sleep(10000);
  }


  @AfterClass
  public static void destroy() throws Exception {
    String[] cmd = { "sh", "-c", "jps -m | grep demo/cars/conf | awk '{print $1}' | xargs kill"};
    Process p = Runtime.getRuntime().exec(cmd);
    Assert.assertEquals(0, p.waitFor());
    cmd = new String []{ "sh", "-c", "jps -m | grep QuorumPeerMain | awk '{print $1}' | xargs kill"};
    p = Runtime.getRuntime().exec(cmd);
    Assert.assertEquals(0, p.waitFor());
  }

  @Test
  public void runDemo() throws Exception {
    // Search
    String bql = "select * from linden browse by color(3) source";
    String result = doGet("http://127.0.0.1:10000/search?bql=" + URLEncoder.encode(bql, "UTF-8"));
    JSONObject jsonResult = JSONObject.parseObject(result);
    System.out.println("Total hits: " + jsonResult.getInteger("totalHits"));
    JSONArray hits = jsonResult.getJSONArray("hits");
    System.out.println("The 1st hit: " + hits.get(0));
    System.out.println("Facet results: " + jsonResult.get("facetResults"));

    // Index
    JSONObject doc = new JSONObject();
    doc.put("id", "NewDocId");
    doc.put("contents", "This is a fake content");

    JSONObject request = new JSONObject();
    request.put("type", "index");
    request.put("content", doc);
    doPost("http://127.0.0.1:10000/index", "content", request.toString());
    Thread.sleep(5000);
    bql = "select * from linden where id = 'NewDocId' source";
    result = doGet("http://127.0.0.1:10000/search?bql=" + URLEncoder.encode(bql, "UTF-8"));
    jsonResult = JSONObject.parseObject(result);
    Assert.assertEquals(1, (int)jsonResult.getInteger("totalHits"));

    // Delete
    request = new JSONObject();
    request.put("type", "delete");
    request.put("id", "NewDocId");
    doPost("http://127.0.0.1:10000/index", "content", request.toString());
    Thread.sleep(5000);
    bql = "select * from linden where id = 'NewDocId' source";
    result = doGet("http://127.0.0.1:10000/search?bql=" + URLEncoder.encode(bql, "UTF-8"));
    jsonResult = JSONObject.parseObject(result);
    Assert.assertEquals(0, (int)jsonResult.getInteger("totalHits"));
  }
}
