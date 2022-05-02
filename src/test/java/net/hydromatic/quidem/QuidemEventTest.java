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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link Quidem}'s push parser.
 */
public class QuidemEventTest {
  /** Unit test for {@link Quidem#isProbablyDeterministic(String)} and in
   * particular for
   * <a href="https://github.com/julianhyde/quidem/issues/7">[QUIDEM-7]
   * Don't be fooled by ORDER BY inside windowed aggregate</a>. */
  @Test void testParse() throws IOException {
    final CountingEventHandler h = new CountingEventHandler();
    final URL hello = QuidemEventTest.class.getResource("/hello.iq");
    assertThat(hello, notNullValue());
    Events.parse(hello, h);
    assertThat(h.okCount, is(1));
    assertThat(h.useCount, is(1));
    assertThat(h.commentCount, is(1));
  }

  /** Implementation of {@link EventHandler} that counts each type of event. */
  private static class CountingEventHandler implements EventHandler {
    int okCount;
    int commentCount;
    int useCount;

    @Override public void comment(List<String> lines) {
      ++commentCount;
    }

    @Override public void use(List<String> lines, String part) {
      ++useCount;
    }

    @Override public void ok(List<String> lines, List<String> content) {
      ++okCount;
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

    @Override public void sort(List<String> content, String sql, boolean sort) {
    }
  }
}

// End QuidemEventTest.java
