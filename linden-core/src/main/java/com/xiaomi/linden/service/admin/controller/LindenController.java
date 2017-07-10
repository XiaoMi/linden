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

package com.xiaomi.linden.service.admin.controller;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.xiaomi.linden.common.util.CommonUtils;
import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.service.admin.LindenAdmin;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenServiceInfo;
import com.xiaomi.linden.thrift.common.Response;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/")
public class LindenController {

  @RequestMapping(value = "/")
  public String getIndexPage() {
    return "index";
  }

  @RequestMapping(value = "/config", method = RequestMethod.GET)
  @ResponseBody
  public LindenConfig getConfig() {
    return LindenAdmin.getService().getLindenConfig();
  }

  @RequestMapping(value = "/service_info", method = RequestMethod.GET)
  @ResponseBody
  public LindenServiceInfo getServiceInfo() throws Exception {
    return Await.result(LindenAdmin.getService().getServiceInfo());
  }

  @RequestMapping(value = "/search", method = RequestMethod.GET)
  @ResponseBody
  public String search(@RequestParam("bql") String bql) {
    LindenResult result;
    try {
      Future<LindenResult> future = LindenAdmin.getService().handleClusterSearchRequest(bql);
      result = Await.result(future, Duration.apply(30000, TimeUnit.MILLISECONDS));
    } catch (Exception e) {
      result = new LindenResult();
      result.setSuccess(false).setError(Throwables.getStackTraceAsString(e));
    }
    return ThriftToJSON(result);
  }

  @RequestMapping(value = "/index", method = RequestMethod.POST)
  @ResponseBody
  public String index(@RequestParam("content") String content) {
    Response response;
    try {
      Future<Response> future = LindenAdmin.getService().handleClusterIndexRequest(content);
      response = Await.result(future, Duration.apply(30000, TimeUnit.MILLISECONDS));
    } catch (Exception e) {
      response = new Response();
      response.setSuccess(false).setError(Throwables.getStackTraceAsString(e));
    }
    return ThriftToJSON(response);
  }

  @RequestMapping(value = "/delete", method = RequestMethod.POST)
  @ResponseBody
  public String delete(@RequestParam("bql") String bql) {
    Response response;
    try {
      Future<Response> future = LindenAdmin.getService().handleClusterDeleteRequest(bql);
      response = Await.result(future, Duration.apply(30000, TimeUnit.MILLISECONDS));
    } catch (Exception e) {
      response = new Response();
      response.setSuccess(false).setError(Throwables.getStackTraceAsString(e));
    }
    return ThriftToJSON(response);
  }

  public static <T extends TBase> String ThriftToJSON(T thrift) {
    TSerializer serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
    try {
      return serializer.toString(thrift);
    } catch (TException e) {
    }
    throw new IllegalStateException("Convert to json failed : " + thrift);
  }
}
