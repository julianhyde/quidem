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

import org.hamcrest.*;
import org.hamcrest.core.SubstringMatcher;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;

import static org.hamcrest.CoreMatchers.containsString;

/**
 * Unit test for {@link Quidem}.
 */
public class QuidemTest {
  @Test public void testBasic() {
    check(
        "!use foodmart\n"
        + "select count(*) as c1 from \"foodmart\".\"days\";\n"
        + "!ok\n"
        + "!set outputformat mysql\n"
        + "select count(*) as c1 from \"foodmart\".\"days\";\n"
        + "!ok\n"
        + "!plan\n"
        + "\n",
        "!use foodmart\n"
        + "select count(*) as c1 from \"foodmart\".\"days\";\n"
        + "C1\n"
        + "7\n"
        + "!ok\n"
        + "!set outputformat mysql\n"
        + "select count(*) as c1 from \"foodmart\".\"days\";\n"
        + "+----+\n"
        + "| C1 |\n"
        + "+----+\n"
        + "| 7  |\n"
        + "+----+\n"
        + "(1 row)\n"
        + "\n"
        + "!ok\n"
        + "isDistinctSelect=[false]\n"
        + "isGrouped=[false]\n"
        + "isAggregated=[true]\n"
        + "columns=[\n"
        + "  COUNT  arg=[   OpTypes.ASTERISK \n"
        + " nullable\n"
        + "\n"
        + "]\n"
        + "[range variable 1\n"
        + "  join type=INNER\n"
        + "  table=days\n"
        + "  cardinality=7\n"
        + "  access=FULL SCAN\n"
        + "  join condition = [index=SYS_IDX_10399\n"
        + "  ]\n"
        + "  ]]\n"
        + "PARAMETERS=[]\n"
        + "SUBQUERIES[]\n"
        + "!plan\n"
        + "\n");
  }

  @Test public void testError() {
    check(
        "!use foodmart\n"
        + "select blah from blah;\n"
        + "!ok\n"
        + "\n",
        containsString(
            "!use foodmart\n"
            + "select blah from blah;\n"
            + "java.sql.SQLSyntaxErrorException: user lacks privilege or object not found: BLAH"));
  }

  @Test public void testPlan() {
    check(
        "!use foodmart\n"
        + "values (1), (2);\n"
        + "!plan\n"
        + "\n",
        matches(
            "(?s)!use foodmart\n"
            + "values \\(1\\), \\(2\\);\n"
            + "isDistinctSelect=.*"
            + "!plan\n"
            + "\n"));
  }

  @Test public void testPlanAfterOk() {
    check(
        "!use foodmart\n"
        + "values (1), (2);\n"
        + "!ok\n"
        + "!plan\n"
        + "\n",
        matches(
            "(?s)!use foodmart\n"
            + "values \\(1\\), \\(2\\);\n"
            + "C1\n"
            + "1\n"
            + "2\n"
            + "!ok\n"
            + "isDistinctSelect=.*"
            + "!plan\n"
            + "\n"));
  }

  /** It is OK to have consecutive '!plan' calls and no '!ok'.
   * (Previously there was a "result already open" error.) */
  @Test public void testPlanPlan() {
    check(
        "!use foodmart\n"
        + "values (1), (2);\n"
        + "!plan\n"
        + "values (3), (4);\n"
        + "!plan\n"
        + "!ok\n"
        + "\n",
        matches(
            "(?s)!use foodmart\n"
            + "values \\(1\\), \\(2\\);\n"
            + "isDistinctSelect=.*\n"
            + "!plan\n"
            + "values \\(3\\), \\(4\\);\n"
            + "isDistinctSelect=.*\n"
            + "!plan\n"
            + "C1\n"
            + "3\n"
            + "4\n"
            + "!ok\n"
            + "\n"));
  }

  /** Content inside a '!ok' command, that needs to be matched. */
  @Test public void testOkContent() {
    check(
        "!use foodmart\n"
        + "values (1), (2);\n"
        + "baz\n"
        + "!ok\n"
        + "\n",
        containsString(
            "!use foodmart\n"
            + "values (1), (2);\n"
            + "C1\n"
            + "1\n"
            + "2\n"
            + "!ok\n"
            + "\n"));
  }

  /** Content inside a '!plan' command, that needs to be matched. */
  @Test public void testPlanContent() {
    check(
        "!use foodmart\n"
        + "values (1), (2);\n"
        + "foo\n"
        + "!plan\n"
        + "baz\n"
        + "!ok\n"
        + "\n",
        matches(
            "(?s)!use foodmart\n"
            + "values \\(1\\), \\(2\\);\n"
            + "isDistinctSelect=.*\n"
            + "!plan\n"
            + "C1\n"
            + "1\n"
            + "2\n"
            + "!ok\n"
            + "\n"));
  }

  @Test public void testIfFalse() {
    check(
        "!use foodmart\n"
        + "!if (false) {\n"
        + "values (1), (2);\n"
        + "anything\n"
        + "you like\n"
        + "!plan\n"
        + "!}\n"
        + "\n",
        containsString(
            "!use foodmart\n"
            + "!if (false) {\n"
            + "values (1), (2);\n"
            + "anything\n"
            + "you like\n"
            + "!plan\n"
            + "!}\n"
            + "\n"));
  }

  @Test public void testIfTrue() {
    check(
        "!use foodmart\n"
        + "!if (true) {\n"
        + "values (1), (2);\n"
        + "anything\n"
        + "you like\n"
        + "!ok\n"
        + "!}\n"
        + "\n",
        containsString(
            "!use foodmart\n"
            + "!if (true) {\n"
            + "values (1), (2);\n"
            + "C1\n"
            + "1\n"
            + "2\n"
            + "!ok\n"
            + "!}\n"
            + "\n"));
  }

  static void check(String input, String expected) {
    check(input, CoreMatchers.equalTo(expected));
  }

  static void check(String input, Matcher<String> matcher) {
    final StringWriter writer = new StringWriter();
    final Quidem run =
        new Quidem(new BufferedReader(new StringReader(input)), writer);
    run.execute(
        new Quidem.ConnectionFactory() {
          public Connection connect(String name) throws Exception {
            if (name.equals("foodmart")) {
              Class.forName("org.hsqldb.jdbcDriver");
              return DriverManager.getConnection("jdbc:hsqldb:res:foodmart",
                  "FOODMART", "FOODMART");
            }
            throw new RuntimeException("unknown connection '" + name + "'");
          }
        });
    writer.flush();
    String out = toLinux(writer.toString());
    Assert.assertThat(out, matcher);
  }

  public static String toLinux(String s) {
    return s.replaceAll("\r\n", "\n");
  }

  public static Matcher<String> matches(String pattern) {
    return new StringMatches(pattern);
  }

  public static class StringMatches extends SubstringMatcher {
    public StringMatches(String pattern) {
      super(pattern);
    }

    @Override protected boolean evalSubstringOf(String s) {
      return s.matches(substring);
    }

    @Override protected String relationship() {
      return "matching";
    }
  }
}

// End QuidemTest.java
