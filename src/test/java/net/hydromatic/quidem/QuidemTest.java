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

import org.junit.Test;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

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
        "!use foodmart\n" + "values (1), (2);\n" + "!ok\n" + "!plan\n" + "\n",
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

  @Test public void testUsage() throws Exception {
    final Matcher<String> matcher =
        startsWith("Usage: quidem argument... inFile outFile");
    checkMain(matcher, "--help");
  }

  @Test public void testDbBad() throws Exception {
    checkMain(startsWith("Insufficient arguments for --db"),
        "--db", "name", "jdbc:url");
  }

  @Test public void testDb() throws Exception {
    final File inFile =
        writeFile("!use fm\nselect * from \"foodmart\".\"days\";\n!ok\n");
    final File outFile = File.createTempFile("outFile", ".iq");
    final Matcher<String> matcher = equalTo("");
    checkMain(matcher, "--db", "fm", "jdbc:hsqldb:res:foodmart", "FOODMART",
        "FOODMART", inFile.getAbsolutePath(), outFile.getAbsolutePath());
    assertThat(contents(outFile),
        equalTo(
            "!use fm\nselect * from \"foodmart\".\"days\";\nday, week_day\n1, Sunday\n2, Monday\n5, Thursday\n4, Wednesday\n3, Tuesday\n6, Friday\n7, Saturday\n!ok\n"));
    inFile.delete();
    outFile.delete();
  }

  private File writeFile(String contents) throws IOException {
    final File inFile = File.createTempFile("inFile", ".iq");
    final FileWriter fw = new FileWriter(inFile);
    fw.append(contents);
    fw.close();
    return inFile;
  }

  @Test public void testFactoryBad() throws Exception {
    checkMain(startsWith("Factory class non.existent.ClassName not found"),
        "--factory", "non.existent.ClassName");
  }

  @Test public void testFactoryBad2() throws Exception {
    checkMain(startsWith("Error instantiating factory class java.lang.String"),
        "--factory", "java.lang.String");
  }

  @Test public void testFactory() throws Exception {
    final File inFile =
        writeFile("!use foo\nvalues 1;\n!ok\n");
    final File outFile = File.createTempFile("outFile", ".iq");
    checkMain(equalTo(""), "--factory", FooFactory.class.getName(),
        inFile.getAbsolutePath(), outFile.getAbsolutePath());
    assertThat(contents(outFile),
        equalTo("!use foo\nvalues 1;\nC1\n1\n!ok\n"));
    inFile.delete();
    outFile.delete();
  }

  private void checkMain(Matcher<String> matcher, String... args)
      throws Exception {
    final StringWriter sw = new StringWriter();
    final PrintWriter pw = new PrintWriter(sw);
    Quidem.main2(Arrays.asList(args), pw);
    pw.close();
    assertThat(sw.toString(), matcher);
  }

  static void check(String input, String expected) {
    check(input, CoreMatchers.equalTo(expected));
  }

  static String contents(File file) throws IOException {
    final FileReader reader = new FileReader(file);
    final StringWriter sw = new StringWriter();
    final char[] buf = new char[1024];
    for (;;) {
      final int read = reader.read(buf, 0, 1024);
      if (read < 0) {
        break;
      }
      sw.write(buf, 0, read);
    }
    return sw.toString();
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
    assertThat(out, matcher);
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

  public static class FooFactory implements Quidem.ConnectionFactory {
    @Override public Connection connect(String name) throws Exception {
      if (name.equals("foo")) {
        return DriverManager.getConnection("jdbc:hsqldb:res:foodmart",
            "FOODMART", "FOODMART");
      }
      return null;
    }
  }
}

// End QuidemTest.java
