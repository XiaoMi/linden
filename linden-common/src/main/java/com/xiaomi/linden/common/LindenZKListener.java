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

package com.xiaomi.linden.common;

import com.github.zkclient.IZkChildListener;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class LindenZKListener implements IZkChildListener {
  private final Set<String> preNodes = new HashSet<>();

  public LindenZKListener(String path, List<String> children) {
    if (children != null) {
      preNodes.addAll(children);
      onChildChange(path, children, children, new ArrayList<String>());
    }
  }

  @Override
  public synchronized void handleChildChange(String parent, List<String> children) throws Exception {
    // handle ZkNoNodeException from ZkClient
    if (children == null) {
      return;
    }

    Set<String> previousNodes = new HashSet<>(preNodes);
    Set<String> currentNodes = new HashSet<>(children);

    // deleted. previous - new nodes
    previousNodes.removeAll(currentNodes);

    // new added. new nodes - previous nodes
    currentNodes.removeAll(preNodes);

    preNodes.addAll(currentNodes);
    preNodes.removeAll(previousNodes);
    List<String> added = new ArrayList<>(currentNodes);
    List<String> deleted = new ArrayList<>(previousNodes);
    onChildChange(parent, children, added, deleted);
  }

  public abstract void onChildChange(String parent, List<String> children, List<String> newAdded, List<String> deleted);
}

