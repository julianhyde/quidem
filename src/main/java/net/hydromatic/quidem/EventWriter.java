/*
 * Licensed to Julian Hyde under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.hydromatic.quidem;

import java.util.List;

/**
 * Implementation of {@link EventHandler} that writes a file in Quidem
 * ({@code .iq}) format.
 *
 * @see Events#write
 */
class EventWriter implements EventHandler {
  private final StringBuilder b;
  private final String sep;

  EventWriter(StringBuilder b, String sep) {
    this.b = b;
    this.sep = sep;
  }

  private void appendAll(Iterable<String> lines) {
    for (String line : lines) {
      b.append(line);
      b.append(sep);
    }
  }

  @Override public void comment(List<String> lines) {
    appendAll(lines);
  }

  @Override public void use(List<String> lines, String part) {
    b.append("!use ").append(part).append(sep);
  }

  @Override public void ok(List<String> lines, List<String> content) {
  }

  @Override public void verify(List<String> lines) {
  }

  @Override public void update(List<String> lines, List<String> content) {
  }

  @Override public void plan(List<String> lines, List<String> content) {
  }

  @Override public void type(List<String> lines, List<String> content) {
  }

  @Override public void error(List<String> lines, List<String> content) {
  }

  @Override public void skip(List<String> lines) {
  }

  @Override public void pop(List<String> lines, Quidem.Property property,
      String propertyName) {
  }

  @Override public void push(List<String> lines, Quidem.Property property,
      String propertyName, Object value) {
  }

  @Override public void set(List<String> lines, Quidem.Property property,
      String propertyName, Object value) {
  }

  @Override public void show(List<String> lines, Quidem.Property property,
      String propertyName) {
  }

  @Override public EventHandler ifBegin(List<String> ifLines,
      List<String> lines, List<String> variables) {
    return this;
  }

  @Override public void ifEnd(EventHandler h2, List<String> ifLines,
      List<String> lines, List<String> variables) {
  }

  @Override public void command(Command command) {
  }

  @Override public void sql(List<String> content, String sql, boolean sort) {
    b.append(sql).append(sep);
  }
}
