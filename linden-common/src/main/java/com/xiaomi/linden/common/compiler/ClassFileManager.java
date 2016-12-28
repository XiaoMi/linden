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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.security.SecureClassLoader;

public class ClassFileManager extends ForwardingJavaFileManager {
  private JavaClassObject jclassObject;

  public ClassFileManager(StandardJavaFileManager
                              standardManager) {
    super(standardManager);
  }

  @Override
  public ClassLoader getClassLoader(Location location) {
    return new SecureClassLoader() {
      @Override
      protected Class<?> findClass(String name)
          throws ClassNotFoundException {
        byte[] b = jclassObject.getBytes();
        return super.defineClass(name, jclassObject
            .getBytes(), 0, b.length);
      }
    };
  }

  @Override
  public JavaFileObject getJavaFileForOutput(Location location,
                                             String className, JavaFileObject.Kind kind, FileObject sibling)
      throws IOException {
    jclassObject = new JavaClassObject(className, kind);
    return jclassObject;
  }
  static public class JavaClassObject extends SimpleJavaFileObject {

    protected final ByteArrayOutputStream bos =
        new ByteArrayOutputStream();

    public JavaClassObject(String name, Kind kind) {
      super(URI.create("string:///" + name.replace('.', '/')
                       + kind.extension), kind);
    }

    public byte[] getBytes() {
      return bos.toByteArray();
    }

    @Override
    public OutputStream openOutputStream() throws IOException {
      return bos;
    }
  }
}
