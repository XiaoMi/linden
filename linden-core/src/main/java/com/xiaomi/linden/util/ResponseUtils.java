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

package com.xiaomi.linden.util;

import com.google.common.base.Throwables;
import com.twitter.util.Future;
import com.xiaomi.linden.thrift.common.Response;

public class ResponseUtils {

  public static final Response SUCCESS = new Response();
  public static final Response FAILED = new Response(false);

  public static Future<Response> buildFailedFutureResponse(Exception e) {
    return Future.value(new Response(false).setError(Throwables.getStackTraceAsString(e)));
  }

  public static Future<Response> buildFailedFutureResponse(String errorStackInfo) {
    return Future.value(new Response(false).setError(errorStackInfo));
  }

  public static Response buildFailedResponse(String error) {
    return new Response(false).setError(error);
  }

  public static Response buildFailedResponse(Exception e) {
    return new Response(false).setError(Throwables.getStackTraceAsString(e));
  }
}
