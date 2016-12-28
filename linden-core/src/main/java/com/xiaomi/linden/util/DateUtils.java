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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
  private static final String CURRENT = "yyyy-MM-dd-HH-mm-ss";
  private static final String HOUR = "yyyy-MM-dd-HH";
  private static final String DAY = "yyyy-MM-dd";
  private static final String MONTH = "yyyy-MM";
  private static final String YEAR = "yyyy";


  public static String getCurrentHour() {
    Date date = new Date();
    SimpleDateFormat format = new SimpleDateFormat(HOUR);
    return format.format(date);
  }

  public static String getCurrentDay() {
    Date date = new Date();
    SimpleDateFormat format = new SimpleDateFormat(DAY);
    return format.format(date);
  }

  public static String getCurrentMonth() {
    Date date = new Date();
    SimpleDateFormat format = new SimpleDateFormat(MONTH);
    return format.format(date);
  }

  public static String getCurrentYear() {
    Date date = new Date();
    SimpleDateFormat format = new SimpleDateFormat(YEAR);
    return format.format(date);
  }

  public static String getCurrentTime() {
    Date date = new Date();
    SimpleDateFormat format = new SimpleDateFormat(CURRENT);
    return format.format(date);
  }
}
