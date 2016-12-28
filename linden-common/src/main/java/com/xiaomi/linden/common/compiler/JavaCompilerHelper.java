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

package com.xiaomi.linden.common.compiler;

import javax.tools.*;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class JavaCompilerHelper {

  public static Class<?> createClass(String className, String content)
      throws Exception {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    JavaFileManager fileManager = new ClassFileManager(compiler.getStandardFileManager(null, null, null));

    // Dynamic compiling requires specifying
    // a list of "files" to compile. In our case
    // this is a list containing one "file" which is in our case
    // our own implementation (see details below)
    List<JavaFileObject> jfiles = new ArrayList<JavaFileObject>();
    jfiles.add(new CharSequenceJavaFileObject(className, content));

    // We specify a task to the compiler. Compiler should use our file
    // manager and our list of "files".
    // Then we run the compilation with call()
    DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();

    boolean sucess = compiler.getTask(null, fileManager, diagnostics, null, null, jfiles).call();
    if (sucess) {
      return fileManager.getClassLoader(null).loadClass(className);
    } else {
      StringWriter error = new StringWriter();
      PrintWriter writer = new PrintWriter(error);
      for (Diagnostic diagnostic : diagnostics.getDiagnostics()) {
        writer.println("Code->" + diagnostic.getCode());
        writer.println("Column Number->" + diagnostic.getColumnNumber());
        writer.println("End Position->" + diagnostic.getEndPosition());
        writer.println("Kind->" + diagnostic.getKind());
        writer.println("Line Number->" + diagnostic.getLineNumber());
        writer.println("Message->" + diagnostic.getMessage(Locale.ENGLISH));
        writer.println("Position->" + diagnostic.getPosition());
        writer.println("Source" + diagnostic.getSource());
        writer.println("Start Position->" + diagnostic.getStartPosition());
        writer.println("\n");
      }
      writer.flush();
      writer.close();
      throw new Exception(error.toString());
    }
  }

  public static void main(String[] args) throws Exception {

    StringBuilder src = new StringBuilder();
    src.append("public class HelloWorld {");
    src.append("  public static void main(String args[]) {");
    src.append("    System.out.println(\"This is in another java file\");");
    src.append("  }");
    src.append("}");
    Class<?> clazz = createClass("HelloWorld", src.toString());
    clazz.getDeclaredMethod("main", new Class[]{ String[].class})
        .invoke(null, new Object[]{null});
  }
}
