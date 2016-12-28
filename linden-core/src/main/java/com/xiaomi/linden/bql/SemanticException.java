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

package com.xiaomi.linden.bql;

import org.antlr.v4.runtime.tree.ParseTree;

public class SemanticException extends Exception {
  private static final long serialVersionUID = 1L;
  private final ParseTree node;

  public SemanticException(ParseTree node, String message) {
    super(message);
    this.node = node;
  }

  public SemanticException(ParseTree node, String message, Throwable cause) {
    super(message, cause);
    this.node = node;
  }

  public ParseTree getNode() {
    return node;
  }
}
