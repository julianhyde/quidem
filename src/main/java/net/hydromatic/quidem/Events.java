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

import com.google.common.collect.ImmutableList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import static net.hydromatic.quidem.Quidem.configBuilder;
import static net.hydromatic.quidem.Quidem.stringIterator;

/** Utilities for parsing and generating files via Quidem's object model. */
public abstract class Events implements EventHandler {
  private Events() {}

  /** Parses a document read from a URL, sending events to the given
   * event handler. */
  public static void parse(URL url, EventHandler h) throws IOException {
    parse(h, new InputStreamReader(url.openStream()));
  }

  /** Parses a document read from a Reader, sending events to the given
   * event handler. */
  public static void parse(EventHandler h, Reader reader) {
    final Parser parser =
        new Parser(configBuilder().withReader(reader).build(), h);
    parser.parse();
  }

  /** Creates an EventHandler that writes to a given StringBuilder. */
  public static EventHandler write(StringBuilder b) {
    return new EventWriter(b);
  }

  /** Parser. */
  private static class Parser {
    final Quidem.Config config;
    final BufferedReader reader;
    final EventHandler h;

    final List<String> lines = new ArrayList<>();
    String pushedLine;
    final StringBuilder buf = new StringBuilder();

    private Parser(Quidem.Config config, EventHandler h) {
      this.config = config;
      final Reader rawReader = config.reader();
      if (rawReader instanceof BufferedReader) {
        this.reader = (BufferedReader) rawReader;
      } else {
        this.reader = new BufferedReader(rawReader);
      }
      this.h = h;
    }

    void parse() {
      for (;;) {
        try {
          if (!nextCommand()) {
            break;
          }
        } catch (IOException e) {
          throw new RuntimeException("Error while reading next command", e);
        }
      }
    }

    private boolean nextCommand() throws IOException {
      lines.clear();
      ImmutableList<String> content = ImmutableList.of();
      for (;;) {
        String line = nextLine();
        if (line == null) {
          return false;
        }
        if (line.startsWith("#") || line.isEmpty()) {
          h.comment(lines);
          return true;
        }
        if (line.startsWith("!")) {
          line = line.substring(1);
          while (line.startsWith(" ")) {
            line = line.substring(1);
          }
          if (line.startsWith("use")) {
            String[] parts = line.split(" ");
            h.use(lines, parts[1]);
            return true;
          }
          if (line.startsWith("ok")) {
            h.ok(lines, content);
            return true;
          }
          if (line.startsWith("verify")) {
            // "content" may or may not be empty. We ignore it.
            // This allows people to switch between '!ok' and '!verify'.
            h.verify(lines);
            return true;
          }
          if (line.startsWith("update")) {
            h.update(lines, content);
            return true;
          }
          if (line.startsWith("plan")) {
            h.plan(lines, content);
            return true;
          }
          if (line.startsWith("type")) {
            h.type(lines, content);
            return true;
          }
          if (line.startsWith("error")) {
            h.error(lines, content);
            return true;
          }
          if (line.startsWith("skip")) {
            h.skip(lines);
            return true;
          }
          if (line.startsWith("pop")) {
            String[] parts = line.split(" ");
            String propertyName = parts[1];
            Quidem.Property property;
            if (propertyName.equals("outputformat")) {
              property = Quidem.Property.OUTPUTFORMAT;
            } else {
              property = Quidem.Property.OTHER;
            }
            h.pop(lines, property, propertyName);
            return true;
          }
          if (line.startsWith("set ") || line.startsWith("push ")) {
            String[] parts = line.split(" ");
            String propertyName = parts[1];
            String valueString = parts[2];
            Object value;
            Quidem.Property property;
            if (propertyName.equals("outputformat")) {
              property = Quidem.Property.OUTPUTFORMAT;
              value = Quidem.OutputFormat.valueOf(parts[2].toUpperCase());
            } else {
              property = Quidem.Property.OTHER;
              if (valueString.equals("null")) {
                value = null;
              } else if (valueString.equals("true")) {
                value = Boolean.TRUE;
              } else if (valueString.equals("false")) {
                value = Boolean.FALSE;
              } else if (valueString.matches("-?[0-9]+")) {
                value = new BigDecimal(valueString);
              } else {
                value = valueString;
              }
            }
            if (line.startsWith("push ")) {
              h.push(lines, property, propertyName, value);
            } else {
              h.set(lines, property, propertyName, value);
            }
            return true;
          }
          if (line.startsWith("show ")) {
            String[] parts = line.split(" ");
            String propertyName = parts[1];
            Quidem.Property property;
            if (propertyName.equals("outputformat")) {
              property = Quidem.Property.OUTPUTFORMAT;
            } else {
              property = Quidem.Property.OTHER;
            }
            h.show(lines, property, propertyName);
            return true;
          }

          if (line.matches("if \\([A-Za-z-][A-Za-z_0-9.]*\\) \\{")) {
            List<String> ifLines = ImmutableList.copyOf(lines);
            lines.clear();
            String variable =
                line.substring("if (".length(),
                    line.length() - ") {".length());
            List<String> variables =
                ImmutableList.copyOf(
                    stringIterator(new StringTokenizer(variable, ".")));
            final EventHandler h2 = h.ifBegin(ifLines, lines, variables);
            final Parser p2 = new Parser(config, h2);
            p2.parse();
            h.ifEnd(h2, ifLines, lines, variables);
            return true;
          }
          if (line.equals("}")) {
            return false;
          }
          final Command command =
              config.commandHandler().parseCommand(lines, content, line);
          if (command != null) {
            h.command(command);
            return true;
          }
          throw new RuntimeException("Unknown command: " + line);
        }
        buf.setLength(0);
        boolean last = false;
        for (;;) {
          if (line.endsWith(";")) {
            last = true;
            line = line.substring(0, line.length() - 1);
          }
          buf.append(line);
          if (last) {
            break;
          }
          buf.append("\n");
          line = nextLine();
          if (line == null) {
            throw new RuntimeException(
                "end of file reached before end of SQL command");
          }
          if (line.startsWith("!") || line.startsWith("#")) {
            pushLine();
            break;
          }
        }
        content = ImmutableList.copyOf(lines);
        lines.clear();
        if (last) {
          String sql = buf.toString();
          final boolean sort = !Quidem.isProbablyDeterministic(sql);
          h.sort(content, sql, sort);
          return true;
        }
      }
    }

    private void pushLine() {
      if (pushedLine != null) {
        throw new AssertionError("cannot push two lines");
      }
      if (lines.size() == 0) {
        throw new AssertionError("no line has been read");
      }
      pushedLine = lines.get(lines.size() - 1);
      lines.remove(lines.size() - 1);
    }

    private String nextLine() throws IOException {
      String line;
      if (pushedLine != null) {
        line = pushedLine;
        pushedLine = null;
      } else {
        line = reader.readLine();
        if (line == null) {
          return null;
        }
      }
      lines.add(line);
      return line;
    }
  }

}
