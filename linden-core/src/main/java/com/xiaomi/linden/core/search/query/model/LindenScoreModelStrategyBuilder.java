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

package com.xiaomi.linden.core.search.query.model;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.xiaomi.linden.common.compiler.JavaCompilerHelper;
import com.xiaomi.linden.lucene.query.flexiblequery.FlexibleQuery;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenInputParam;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenScoreModel;
import com.xiaomi.linden.thrift.common.LindenValue;

public class LindenScoreModelStrategyBuilder {

  private static final HashSet<String> importMap = Sets.newHashSet();
  private static final Map<String, Class<?>> compiledClassMap = Maps.newConcurrentMap();
  private static final Map<String, String> failedClassMap = Maps.newConcurrentMap();
  private static final Map<String, Class<?>> pluginClassMap = Maps.newConcurrentMap();
  private static final String LATITUDE = "latitude";
  private static final String LONGITUDE = "longitude";
  private static volatile URLClassLoader urlClassLoader;

  static {
    importMap.add("com.spatial4j.core.distance.DistanceUtils");
    importMap.add("com.xiaomi.linden.core.search.query.model.LindenScoreModelStrategy");
    importMap.add("com.xiaomi.linden.lucene.query.flexiblequery.FlexibleScoreModelStrategy");
    importMap.add("org.apache.lucene.search.Explanation");
    importMap.add("org.apache.lucene.search.Query");
    importMap.add("org.apache.lucene.search.FieldCache");
    importMap.add("org.apache.lucene.index.BinaryDocValues");
    importMap.add("java.io.IOException");
    importMap.add("java.util.List");
    importMap.add("java.util.Map");
    importMap.add("java.util.HashMap");
    importMap.add("com.xiaomi.linden.thrift.common.LindenValue");
    importMap.add("com.xiaomi.linden.thrift.common.LindenInputParam");
    importMap.add("com.xiaomi.linden.lucene.search.LindenFieldCacheImpl");
    importMap.add("org.apache.lucene.index.AtomicReaderContext");
  }

  private static final String CLASS_HEADER_FORMAT = "public class %s extends LindenScoreModelStrategy";
  private static final String FLEXIBLE_QUERY_CLASS_HEADER_FORMAT = "public class %s extends FlexibleScoreModelStrategy";
  private static final String SCORE_METHOD_FORMAT =
      "  @Override\n" +
      "  public double computeScore() throws IOException {\n" +
      "  %s\n" +
      "  }\n";

  private static final String DISTANCE_METHOD =
      "   public float distance() throws IOException {\n" +
      "    if (getCoordinate() == null) return 0f;\n" +
      "    \n" +
      "    double distance = DistanceUtils.distLawOfCosinesRAD(\n" +
      "        DistanceUtils.toRadians(getCoordinate().getLatitude()),\n" +
      "        DistanceUtils.toRadians(getCoordinate().getLongitude()),\n" +
      "        DistanceUtils.toRadians(latitude()),\n" +
      "        DistanceUtils.toRadians(longitude()));\n" +
      "    return (float) DistanceUtils.radians2Dist(distance, DistanceUtils.EARTH_MEAN_RADIUS_KM);\n" +
      "  }";

  private static String getImportHeader() {
    StringWriter writer = new StringWriter();
    PrintWriter printer = new PrintWriter(writer);
    for (String pkg : importMap) {
      printer.printf("import %s;\n", pkg);
    }
    printer.flush();
    printer.close();
    return writer.toString();
  }

  private static Class<?> loadClass(String path, String modelName) throws Exception {
    if (urlClassLoader == null) {
      synchronized (URLClassLoader.class) {
        if (urlClassLoader == null) {
          File[] files = new File(path).listFiles();
          URL urls[] = new URL[files.length];
          for (int i = 0; i < files.length; ++i) {
            urls[i] = files[i].toURI().toURL();
          }
          urlClassLoader = new URLClassLoader(urls);
        }
      }
    }
    return Class.forName(modelName, true, urlClassLoader);
  }

  public static LindenScoreModelStrategy buildFlexibleQueryStrategy(FlexibleQuery query) throws Exception {
    return build(FLEXIBLE_QUERY_CLASS_HEADER_FORMAT, query.getConfig().getPluginPath(), query.getModel(),
                 query.getConfig().getSchema());
  }

  public static LindenScoreModelStrategy build(String pluginPath, LindenScoreModel model, LindenSchema schema)
      throws Exception {
    return build(CLASS_HEADER_FORMAT, pluginPath, model, schema);
  }

  public static LindenScoreModelStrategy build(String classHeaderFormat, String pluginPath, LindenScoreModel model,
                                               LindenSchema schema) throws Exception {
    if (model.isPlugin()) {
      if (pluginPath == null) {
        throw new Exception("Does not support plugin. Plugin path is null");
      }
      Class<?> clazz;
      if (model.isOverride()) {
        clazz = loadClass(pluginPath, model.getName());
      } else {
        clazz = pluginClassMap.get(model.getName());
        if (clazz != null) {
          return (LindenScoreModelStrategy) clazz.newInstance();
        } else {
          clazz = loadClass(pluginPath, model.getName());
        }
      }
      if (clazz != null) {
        pluginClassMap.put(model.getName(), clazz);
        return (LindenScoreModelStrategy) clazz.newInstance();
      } else {
        throw new Exception("Plugin " + model.getName() + " not found.");
      }
    }

    StringBuilder fingerPrintBuilder = new StringBuilder();
    for (int i = 0; i < model.getParamsSize(); ++i) {
      if (!model.getParams().get(i).isSetValue()) {
        continue;
      }
      fingerPrintBuilder.append(i);
      fingerPrintBuilder.append(model.getParams().get(i).getName());
      LindenValue value = model.getParams().get(i).getValue();
      String paramType = "";
      if (value.isSetStringValue()) {
        paramType = "String";
      } else if (value.isSetLongValue()) {
        paramType = "Long";
      } else if (value.isSetDoubleValue()) {
        paramType = "Double";
      } else if (value.isSetStringValues()) {
        paramType = "StringList";
      } else if (value.isSetLongValues()) {
        paramType = "LongList";
      } else if (value.isSetDoubleValues()) {
        paramType = "DoubleList";
      } else if (value.isSetMapValue()) {
        paramType = "Map";
      }
      fingerPrintBuilder.append(paramType);
    }
    fingerPrintBuilder.append(model.getFunc());
    long hash = fingerPrintBuilder.toString().hashCode();
    String className = "LSM" + Math.abs(hash);
    Class<?> clazz = compiledClassMap.get(className);
    if (clazz != null) {
      return (LindenScoreModelStrategy) clazz.newInstance();
    } else {
      if (failedClassMap.containsKey(className)) {
        throw new Exception("score model " + model.getName() + " compile failed, please check score model code");
      }
      StringWriter classBody = new StringWriter();
      PrintWriter classPrinter = new PrintWriter(classBody);
      String classHeader = String.format(classHeaderFormat, className);
      // header
      classPrinter.println(getImportHeader());
      classPrinter.println(classHeader + "{");

      // model method
      StringBuilder scoreBody = new StringBuilder();
      if (model.isSetParams() && !model.getParams().isEmpty()) {
        String scoreMethodParamCode = buildInputParamCode(model.params);
        scoreBody.append(scoreMethodParamCode);
      }
      scoreBody.append(model.getFunc());
      classPrinter.printf(SCORE_METHOD_FORMAT, scoreBody.toString());

      if (schema != null) {
        boolean hasId = false;
        for (LindenFieldSchema field : schema.getFields()) {
          if (field.getName().equals(schema.getId())) {
            hasId = true;
          }
          String retType;
          if (field.isMulti()) {
            switch (field.getType()) {
              case INTEGER:
                retType = "int[]";
                break;
              case LONG:
                retType = "long[]";
                break;
              case FLOAT:
                retType = "float[]";
                break;
              case DOUBLE:
                retType = "double[]";
                break;
              case STRING:
              case FACET:
                retType = "String[]";
                break;
              default:
                continue;
            }
          } else {
            switch (field.getType()) {
              case INTEGER:
                retType = "Integer";
                break;
              case LONG:
                retType = "Long";
                break;
              case DOUBLE:
                retType = "Double";
                break;
              case FLOAT:
                retType = "Float";
                break;
              case STRING:
              case FACET:
                retType = "String";
                break;
              default:
                continue;
            }
          }
          classPrinter.printf("private FieldValues<%s> %s;\n", retType, field.getName());
          classPrinter.printf("public %s %s() throws IOException {\n" +
                              "    if (%s == null) {\n" +
                              "      %s = getFieldValues(\"%s\");\n" +
                              "    }\n" +
                              "    return %s.get();\n" +
                              "  }\n", retType, field.getName(), field.getName(), field.getName(), field.getName(),
                              field.getName());
        }

        // add id field, since id field may not be in schema.getFields()
        if (!hasId) {
          classPrinter.printf("private FieldValues<String> %s;\n", schema.getId());
          classPrinter.printf("public String %s() throws IOException {\n" +
                              "    if (%s == null) {\n" +
                              "      %s = getFieldValues(\"%s\");\n" +
                              "    }\n" +
                              "    return %s.get();\n" +
                              "  }\n", schema.getId(), schema.getId(), schema.getId(), schema.getId(), schema.getId());
        }

        if (isDistanceMethodNeeded(schema)) {
          classPrinter.append(DISTANCE_METHOD);
        }
      }
      classPrinter.println("}");
      try {
        clazz = JavaCompilerHelper.createClass(className, classBody.toString());
      } catch (Exception e) {
        failedClassMap.put(className, e.getMessage());
        throw new Exception(Throwables.getStackTraceAsString(e) + "\n" + classBody.toString());
      }
      compiledClassMap.put(className, clazz);
      return (LindenScoreModelStrategy) clazz.newInstance();
    }
  }

  private static boolean isDistanceMethodNeeded(LindenSchema schema) {
    if (schema == null) {
      return false;
    }
    boolean hasLat = false;
    boolean hasLon = false;
    for (LindenFieldSchema fieldSchema : schema.getFields()) {
      if (fieldSchema.getName().equals(LATITUDE)) {
        hasLat = true;
      }
      if (fieldSchema.getName().equals(LONGITUDE)) {
        hasLon = true;
      }
    }
    return hasLat && hasLon;
  }

  private static String buildInputParamCode(List<LindenInputParam> params) throws Exception {
    StringBuilder sb = new StringBuilder();
    if (params != null && !params.isEmpty()) {
      sb.append("if (getParams() == null || getParams().isEmpty()) {\n");
      sb.append("    return 0f;\n   }\n");
      for (int i = 0; i < params.size(); ++i) {
        LindenInputParam param = params.get(i);
        if (!param.isSetValue()) {
          continue;
        }
        if (param.getValue().isSetStringValue()) {
          sb.append(
              String.format("String %s = getParams().get(%d).getValue().getStringValue();\n", param.getName(), i));
        } else if (param.getValue().isSetLongValue()) {
          sb.append(String.format("Long %s = getParams().get(%d).getValue().getLongValue();\n", param.getName(), i));
        } else if (param.getValue().isSetDoubleValue()) {
          sb.append(
              String.format("Double %s = getParams().get(%d).getValue().getDoubleValue();\n", param.getName(), i));
        } else if (param.getValue().isSetLongValues()) {
          sb.append(
              String.format("List<Long> %s = getParams().get(%d).getValue().getLongValues();\n", param.getName(), i));
        } else if (param.getValue().isSetDoubleValues()) {
          sb.append(String.format("List<Double> %s = getParams().get(%d).getValue().getDoubleValues();\n",
                                  param.getName(), i));
        } else if (param.getValue().isSetStringValues()) {
          sb.append(String.format("List<String> %s = getParams().get(%d).getValue().getStringValues();\n",
                                  param.getName(), i));
        } else if (param.getValue().isSetMapValue()) {
          LindenValueTypeMethodPair keyPair = null, valuePair = null;
          for (Map.Entry<LindenValue, LindenValue> entry : param.getValue().getMapValue().entrySet()) {
            keyPair = parseLindenValueTypeMethodPair(entry.getKey());
            valuePair = parseLindenValueTypeMethodPair(entry.getValue());
            break;
          }
          if (keyPair != null && valuePair != null) {
            sb.append(String.format("Map<%s, %s> %s = new HashMap<>();\n",
                                    keyPair.type, valuePair.type, param.getName()));
            sb.append(String.format(
                "Map<LindenValue, LindenValue> %sMapsLocal = getParams().get(%d).getValue().getMapValue();\n",
                param.getName(), i));
            sb.append(String.format("for(Map.Entry<LindenValue, LindenValue> entry : %sMapsLocal.entrySet()) {\n",
                                    param.getName()));
            sb.append(String.format("%s.put(entry.getKey().%s, entry.getValue().%s);\n",
                                    param.getName(), keyPair.method, valuePair.method));
            sb.append(String.format("}\n"));
          } else {
            sb.append(String.format("Map<Object, Object> %s = new HashMap<>();\n", param.getName()));
          }
        }
      }
    }
    return sb.toString();
  }

  private static LindenValueTypeMethodPair parseLindenValueTypeMethodPair(LindenValue value) throws Exception {
    LindenValueTypeMethodPair pair = new LindenValueTypeMethodPair();
    if (value.isSetStringValue()) {
      pair.type = "String";
      pair.method = "getStringValue()";
    } else if (value.isSetLongValue()) {
      pair.type = "Long";
      pair.method = "getLongValue()";
    } else if (value.isSetDoubleValue()) {
      pair.type = "Double";
      pair.method = "getDoubleValue()";
    } else if (value.isSetStringValues()) {
      pair.type = "List<String>";
      pair.method = "getStringValues()";
    } else if (value.isSetLongValues()) {
      pair.type = "List<Long>";
      pair.method = "getLongValues()";
    } else if (value.isSetDoubleValues()) {
      pair.type = "List<Double>";
      pair.method = "getDoubleValues()";
    } else {
      throw new Exception("Doesn't support this linden value typ: " + value.toString());
    }
    return pair;
  }

  public static class LindenValueTypeMethodPair {

    public String type;
    public String method;
  }
}
