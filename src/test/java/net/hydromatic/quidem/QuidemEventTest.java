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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link Quidem}'s push parser and object model.
 */
public class QuidemEventTest {
  /** Reads the contents of a URL as a string. */
  private static String readUrl(URL url) throws IOException {
    final StringWriter b = new StringWriter();
    try (InputStream x = url.openStream();
         Reader r = new InputStreamReader(x)) {
      for (;;) {
        final int c = r.read();
        if (c == -1) {
          return b.toString();
        }
        b.write(c);
      }
    }
  }

  /** Unit test for {@link Events#parse(URL, EventHandler)} on a simple file. */
  @Test void testParse() throws IOException {
    final URL url = QuidemEventTest.class.getResource("/hello.iq");
    assertThat(url, notNullValue());
    final CountingEventHandler h = new CountingEventHandler();
    Events.parse(url, h);
    final String expected = "{"
        + "comment=1, "
        + "ok=1, "
        + "sql=1, "
        + "use=1}";
    final String s = new TreeMap<>(h.map).toString();
    assertThat(s, is(expected));
  }

  /** Unit test for {@link Events#parse(URL, EventHandler)} on a more
   * comprehensive file. */
  @Test void testParse2() throws IOException {
    final URL url = QuidemEventTest.class.getResource("/all.iq");
    assertThat(url, notNullValue());
    final CountingEventHandler h = new CountingEventHandler();
    Events.parse(url, h);
    final String expected = "{"
        + "comment=3, "
        + "error=1, "
        + "ifBegin=2, "
        + "ifEnd=2, "
        + "ok=3, "
        + "plan=1, "
        + "pop=1, "
        + "push=1, "
        + "set=2, "
        + "show=1, "
        + "sql=5, "
        + "type=1, "
        + "update=1, "
        + "use=1, "
        + "verify=1}";
    final String s = new TreeMap<>(h.map).toString();
    assertThat(s, is(expected));
  }

  /** Unit test for {@link Events#write(StringBuilder, String)}. */
  @Test public void testWrite() throws IOException {
    final URL url = QuidemEventTest.class.getResource("/all.iq");
    assertThat(url, notNullValue());
    final StringBuilder b = new StringBuilder();
    final EventHandler h = Events.write(b, "\n");
    Events.parse(url, h);
    final String expectedStart =
        "# Quidem script that has at least one example of each command";
    final String expected = readUrl(url);
    assertThat(expected, startsWith(expectedStart));
    assertThat(b.toString(), is(expected));
  }

  /** Implementation of {@link EventHandler} that counts each type of event. */
  private static class CountingEventHandler implements EventHandler {
    final Map<String, Integer> map = new HashMap<>();

    private void count(String key) {
      map.compute(key, (k, v) -> (v == null ? 0 : v) + 1);
    }

    @Override public void comment(List<String> lines) {
      count("comment");
    }

    @Override public void use(List<String> lines, String part) {
      count("use");
    }

    @Override public void ok(List<String> lines, List<String> content) {
      count("ok");
    }

    @Override public void verify(List<String> lines) {
      count("verify");
    }

    @Override public void update(List<String> lines, List<String> content) {
      count("update");
    }

    @Override public void plan(List<String> lines, List<String> content) {
      count("plan");
    }

    @Override public void type(List<String> lines, List<String> content) {
      count("type");
    }

    @Override public void error(List<String> lines, List<String> content) {
      count("error");
    }

    @Override public void skip(List<String> lines) {
      count("skip");
    }

    @Override public void pop(List<String> lines, Quidem.Property property,
        String propertyName) {
      count("pop");
    }

    @Override public void push(List<String> lines, Quidem.Property property,
        String propertyName, Object value) {
      count("push");
    }

    @Override public void set(List<String> lines, Quidem.Property property,
        String propertyName, Object value) {
      count("set");
    }

    @Override public void show(List<String> lines, Quidem.Property property,
        String propertyName) {
      count("show");
    }

    @Override public EventHandler ifBegin(List<String> ifLines,
        List<String> lines, List<String> variables) {
      count("ifBegin");
      return this;
    }

    @Override public void ifEnd(EventHandler h2, List<String> ifLines,
        List<String> lines, List<String> variables) {
      count("ifEnd");
    }

    @Override public void command(Command command) {
      count("command");
    }

    @Override public void sql(List<String> content, String sql, boolean sort) {
      count("sql");
    }
  }
}

// End QuidemEventTest.java
