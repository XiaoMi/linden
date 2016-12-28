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

package com.xiaomi.linden.service.resource;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.server.ServerConfig;

@Path("/")
public class LindenHttpResource {
  @Context
  private ServerConfig serverConfig;

  @Path("/search")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public String search(@QueryParam("query") String query) {
    /*
    JSONObject obj = (JSONObject) JSONObject.parse(query);
    LindenQuery lindenQuery = new LindenQuery(JSONQueryConverter
            .convertToQuery(obj));
    LindenSearchRequest request = new LindenSearchRequest().setQuery(lindenQuery);
    Future<LindenResult> res = LindenHttpServer.lindenService.search(request);
    LindenResult lindenResult = null;
    try {
      lindenResult = Await.result(res, Duration.apply(
              Long.valueOf(serverConfig.getProperty(LindenHttpServer.TIMEOUT).toString()), TimeUnit.MILLISECONDS));
    } catch (Exception e) {
      lindenResult = new LindenResult().setSuccess(false).setError(Throwables.getStackTraceAsString(e));
    }
    return CommonUtils.ThriftToJSON(lindenResult);
    */
    return null;
  }

  @Path("/index")
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  public String index(@QueryParam("content") String content) {
    /*
    Future<Response> res = LindenHttpServer.lindenService.handleRequest(content);
    Response lindenResponse = null;
    try {
      lindenResponse = Await.result(res, Duration.apply(
              Long.valueOf(serverConfig.getProperty(LindenHttpServer.TIMEOUT).toString()), TimeUnit.MILLISECONDS));
    } catch (Exception e) {
      lindenResponse = new Response().setSuccess(false).setError(Throwables.getStackTraceAsString(e));
    }
    return CommonUtils.ThriftToJSON(lindenResponse);
    */
    return null;
  }

}
