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
        "!use scott\n"
        + "select count(*) as c from scott.emp;\n"
        + "!ok\n"
        + "!set outputformat mysql\n"
        + "select count(*) as c from scott.emp;\n"
        + "!ok\n"
        + "!plan\n"
        + "\n")
        .outputs(
            "!use scott\n"
                + "select count(*) as c from scott.emp;\n"
                + "C\n"
                + "14\n"
                + "!ok\n"
                + "!set outputformat mysql\n"
                + "select count(*) as c from scott.emp;\n"
                + "+----+\n"
                + "| C  |\n"
                + "+----+\n"
                + "| 14 |\n"
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
                + "  table=EMP\n"
                + "  cardinality=14\n"
                + "  access=FULL SCAN\n"
                + "  join condition = [index=SYS_IDX_10095\n"
                + "  ]\n"
                + "  ]]\n"
                + "PARAMETERS=[]\n"
                + "SUBQUERIES[]\n"
                + "!plan\n"
                + "\n");
  }

  @Test public void testError() {
    check(
        "!use scott\n"
        + "select blah from blah;\n"
        + "!ok\n"
        + "\n")
        .contains(
            "!use scott\n"
            + "select blah from blah;\n"
            + "java.sql.SQLSyntaxErrorException: user lacks privilege or object not found: BLAH");
  }

  @Test public void testExpectError() {
    check(
        "!use scott\n"
            + "select blah from blah;\n"
            + "user lacks privilege or object not found: BLAH\n"
            + "!error\n"
            + "\n")
        .matches(
            "(?s)!use scott\n"
                + "select blah from blah;\n"
                + "user lacks privilege or object not found: BLAH\n"
                + "!error\n"
                + "\n");
  }

  @Test public void testExpectErrorNoExpected() {
    check(
        "!use scott\n"
            + "select blah from blah;\n"
            + "!error\n"
            + "\n")
        .matches(
            "(?s)!use scott\n"
                + "select blah from blah;\n"
                + "java.sql.SQLSyntaxErrorException: user lacks privilege or object not found: BLAH\n"
                + "\tat org.hsqldb.jdbc.JDBCUtil.sqlException\\(Unknown Source\\)\n"
                + "\tat org.hsqldb.jdbc.JDBCUtil.sqlException\\(Unknown Source\\)\n"
                + "\tat org.hsqldb.jdbc.JDBCStatement.fetchResult\\(Unknown Source\\)\n"
                + ".*"
                + "!error\n"
                + "\n");
  }

  @Test public void testExpectErrorPermissiveTabs() {
    // Quidem matches even though there are differences in tabs, multiple
    // spaces, spaces at the start of lines, and different line endings.
    // Quidem converts line endings to linux.
    check(
        "!use scott\n"
            + "select blah from blah;\n"
            + "java.sql.SQLSyntaxErrorException: user lacks privilege or object not found: BLAH \n"
            + "\tat org.hsqldb.jdbc.JDBCUtil.sqlException(Unknown Source)\n"
            + "  at  org.hsqldb.jdbc.JDBCUtil.sqlException(Unknown Source)\r\n"
            + "at org.hsqldb.jdbc.JDBCStatement.fetchResult(Unknown Source)  \n"
            + "!error\n"
            + "\n")
        .matches(
            "(?s)!use scott\n"
                + "select blah from blah;\n"
                + "java.sql.SQLSyntaxErrorException: user lacks privilege or object not found: BLAH \n"
                + "\tat org.hsqldb.jdbc.JDBCUtil.sqlException\\(Unknown Source\\)\n"
                + "  at  org.hsqldb.jdbc.JDBCUtil.sqlException\\(Unknown Source\\)\n"
                + "at org.hsqldb.jdbc.JDBCStatement.fetchResult\\(Unknown Source\\)  \n"
                + "!error\n"
                + "\n");
  }

  @Test public void testExpectErrorDifferent() {
    check(
        "!use scott\n"
            + "select blah from blah;\n"
            + "user lacks bizz buzz\n"
            + "!error\n"
            + "\n")
        .matches(
            "(?s)!use scott\n"
                + "select blah from blah;\n"
                + "java.sql.SQLSyntaxErrorException: user lacks privilege or object not found: BLAH\n"
                + "\tat org.hsqldb.jdbc.JDBCUtil.sqlException\\(Unknown Source\\)\n"
                + ".*"
                + " more\n"
                + "!error\n"
                + "\n");
  }

  @Test public void testPlan() {
    check(
        "!use scott\n"
        + "values (1), (2);\n"
        + "!plan\n"
        + "\n")
        .matches(
            "(?s)!use scott\n"
                + "values \\(1\\), \\(2\\);\n"
                + "isDistinctSelect=.*"
                + "!plan\n"
                + "\n");
  }

  @Test public void testPlanAfterOk() {
    check(
        "!use scott\n"
        + "values (1), (2);\n"
        + "!ok\n"
        + "!plan\n"
        + "\n")
        .matches(
            "(?s)!use scott\n"
                + "values \\(1\\), \\(2\\);\n"
                + "C1\n"
                + "1\n"
                + "2\n"
                + "!ok\n"
                + "isDistinctSelect=.*"
                + "!plan\n"
                + "\n");
  }

  /** It is OK to have consecutive '!plan' calls and no '!ok'.
   * (Previously there was a "result already open" error.) */
  @Test public void testPlanPlan() {
    check(
        "!use scott\n"
        + "values (1), (2);\n"
        + "!plan\n"
        + "values (3), (4);\n"
        + "!plan\n"
        + "!ok\n"
        + "\n")
        .matches(
            "(?s)!use scott\n"
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
                + "\n");
  }

  /** Content inside a '!ok' command, that needs to be matched. */
  @Test public void testOkContent() {
    check(
        "!use scott\n"
        + "values (1), (2);\n"
        + "baz\n"
        + "!ok\n"
        + "\n")
        .contains(
            "!use scott\n"
                + "values (1), (2);\n"
                + "C1\n"
                + "1\n"
                + "2\n"
                + "!ok\n"
                + "\n");
  }

  /** If the statement contains 'order by', result is not re-ordered to match
   * the input string. */
  @Test public void testOkOrderBy() {
    // In (2, 1), out (1, 2). Test gives a diff (correctly).
    check("!use scott\n"
        + "select * from (values (1), (2)) as t(c) order by 1;\n"
        + "C\n"
        + "2\n"
        + "1\n"
        + "!ok\n"
        + "\n")
        .contains(
            "!use scott\n"
                + "select * from (values (1), (2)) as t(c) order by 1;\n"
                + "C\n"
                + "1\n"
                + "2\n"
                + "!ok\n"
                + "\n");
    // In (1, 2), out (1, 2). Test passes.
    check("!use scott\n"
        + "select * from (values (1), (2)) as t(c) order by 1;\n"
        + "C\n"
        + "1\n"
        + "2\n"
        + "!ok\n"
        + "\n")
        .contains(
            "!use scott\n"
                + "select * from (values (1), (2)) as t(c) order by 1;\n"
                + "C\n"
                + "1\n"
                + "2\n"
                + "!ok\n"
                + "\n");
  }

  /** As {@link #testOkOrderBy()} but for MySQL. */
  @Test public void testOkOrderByMySQL() {
    // In (2, 1), out (1, 2). Test gives a diff (correctly).
    check("!use scott\n"
        + "!set outputformat mysql\n"
        + "select * from (values (1), (2)) as t(c) order by 1;\n"
        + "+---+\n"
        + "| C |\n"
        + "+---+\n"
        + "| 2 |\n"
        + "| 1 |\n"
        + "+---+\n"
        + "(2 rows)\n"
        + "\n"
        + "!ok\n"
        + "\n")
        .contains(
            "!use scott\n"
                + "!set outputformat mysql\n"
                + "select * from (values (1), (2)) as t(c) order by 1;\n"
                + "+---+\n"
                + "| C |\n"
                + "+---+\n"
                + "| 1 |\n"
                + "| 2 |\n"
                + "+---+\n"
                + "(2 rows)\n"
                + "\n"
                + "!ok\n"
                + "\n");
    // In (1, 2), out (1, 2). Test passes.
    check("!use scott\n"
        + "!set outputformat mysql\n"
        + "select * from (values (1), (2)) as t(c) order by 1;\n"
        + "+---+\n"
        + "| C |\n"
        + "+---+\n"
        + "| 1 |\n"
        + "| 2 |\n"
        + "+---+\n"
        + "(2 rows)\n"
        + "\n"
        + "!ok\n"
        + "\n")
        .contains(
            "!use scott\n"
                + "!set outputformat mysql\n"
                + "select * from (values (1), (2)) as t(c) order by 1;\n"
                + "+---+\n"
                + "| C |\n"
                + "+---+\n"
                + "| 1 |\n"
                + "| 2 |\n"
                + "+---+\n"
                + "(2 rows)\n"
                + "\n"
                + "!ok\n"
                + "\n");
  }

  /** If the statement does not contain 'order by', result is re-ordered to
   * match the input string. */
  @Test public void testOkNoOrderBy() {
    // In (2, 1), out (2, 1). Result would be correct in either order, but
    // we output in the original order, so as not to cause a diff.
    check(
        "!use scott\n"
        + "select * from (values (1), (2)) as t(c);\n"
        + "C\n"
        + "2\n"
        + "1\n"
        + "!ok\n"
        + "\n")
        .contains(
            "!use scott\n"
                + "select * from (values (1), (2)) as t(c);\n"
                + "C\n"
                + "2\n"
                + "1\n"
                + "!ok\n"
                + "\n");
    // In (1, 2), out (1, 2).
    check(
        "!use scott\n"
        + "select * from (values (1), (2)) as t(c);\n"
        + "C\n"
        + "1\n"
        + "2\n"
        + "!ok\n"
        + "\n")
        .contains(
            "!use scott\n"
                + "select * from (values (1), (2)) as t(c);\n"
                + "C\n"
                + "1\n"
                + "2\n"
                + "!ok\n"
                + "\n");
  }

  /** Content inside a '!plan' command, that needs to be matched. */
  @Test public void testPlanContent() {
    check(
        "!use scott\n"
        + "values (1), (2);\n"
        + "foo\n"
        + "!plan\n"
        + "baz\n"
        + "!ok\n"
        + "\n")
        .matches(
            "(?s)!use scott\n"
                + "values \\(1\\), \\(2\\);\n"
                + "isDistinctSelect=.*\n"
                + "!plan\n"
                + "C1\n"
                + "1\n"
                + "2\n"
                + "!ok\n"
                + "\n");
  }

  @Test public void testIfFalse() {
    check(
        "!use scott\n"
        + "!if (false) {\n"
        + "values (1), (2);\n"
        + "anything\n"
        + "you like\n"
        + "!plan\n"
        + "!}\n"
        + "\n")
        .contains(
            "!use scott\n"
                + "!if (false) {\n"
                + "values (1), (2);\n"
                + "anything\n"
                + "you like\n"
                + "!plan\n"
                + "!}\n"
                + "\n");
  }

  @Test public void testIfTrue() {
    check(
        "!use scott\n"
        + "!if (true) {\n"
        + "values (1), (2);\n"
        + "anything\n"
        + "you like\n"
        + "!ok\n"
        + "!}\n"
        + "\n")
        .contains(
            "!use scott\n"
                + "!if (true) {\n"
                + "values (1), (2);\n"
                + "C1\n"
                + "1\n"
                + "2\n"
                + "!ok\n"
                + "!}\n"
                + "\n");
  }

  @Test public void testSkip() {
    check(
        "!use scott\n"
        + "!skip\n"
        + "values (1);\n"
        + "anything\n"
        + "!ok\n"
        + "values (1);\n"
        + "you like\n"
        + "!error\n"
        + "\n")
        .contains(
            "!use scott\n"
                + "!skip\n"
                + "values (1);\n"
                + "anything\n"
                + "!ok\n"
                + "values (1);\n"
                + "you like\n"
                + "!error\n"
                + "\n");
  }

  @Test public void testJustify() {
    check(
        "!use scott\n"
            + "select true as b00000,\n"
            + "  cast(1 as tinyint) as t000,\n"
            + "  cast(1 as integer) as i000,\n"
            + "  cast(1 as float) as f00000,\n"
            + "  cast(1 as double) as d00000,\n"
            + "  cast(1 as varchar(3)) as v003\n"
            + "from (values (1));\n"
            + "!set outputformat mysql\n"
            + "!ok\n"
            + "!set outputformat psql\n"
            + "!ok\n"
            + "!set outputformat csv\n"
            + "!ok\n"
            + "\n")
        .contains("!use scott\n"
            + "select true as b00000,\n"
            + "  cast(1 as tinyint) as t000,\n"
            + "  cast(1 as integer) as i000,\n"
            + "  cast(1 as float) as f00000,\n"
            + "  cast(1 as double) as d00000,\n"
            + "  cast(1 as varchar(3)) as v003\n"
            + "from (values (1));\n"
            + "!set outputformat mysql\n"
            + "+--------+------+------+--------+--------+------+\n"
            + "| B00000 | T000 | I000 | F00000 | D00000 | V003 |\n"
            + "+--------+------+------+--------+--------+------+\n"
            + "| TRUE   |    1 |    1 |  1.0E0 |  1.0E0 | 1    |\n"
            + "+--------+------+------+--------+--------+------+\n"
            + "(1 row)\n"
            + "\n"
            + "!ok\n"
            + "!set outputformat psql\n"
            + " B00000 | T000 | I000 | F00000 | D00000 | V003\n"
            + "--------+------+------+--------+--------+------\n"
            + " TRUE   |    1 |    1 |  1.0E0 |  1.0E0 | 1\n"
            + "(1 row)\n"
            + "\n"
            + "!ok\n"
            + "!set outputformat csv\n"
            + "B00000, T000, I000, F00000, D00000, V003\n"
            + "TRUE, 1, 1, 1.0E0, 1.0E0, 1\n"
            + "!ok\n"
            + "\n");
  }

  /** Test case for
   * <a href="https://github.com/julianhyde/quidem/issues/3">[QUIDEM-3]
   * Trailing spaces in psql output format</a>. */
  @Test public void testColumnHeading() {
    // Note: There must not be trailing spaces after 'DEPTNO | B'
    check(
        "!use scott\n"
            + "!set outputformat psql\n"
            + "select deptno, deptno > 20 as b from scott.dept order by 1;\n"
            + "!ok\n"
            + "\n")
        .contains("!use scott\n"
                + "!set outputformat psql\n"
                + "select deptno, deptno > 20 as b from scott.dept order by 1;\n"
                + " DEPTNO | B\n"
                + "--------+-------\n"
                + "     10 | FALSE\n"
                + "     20 | FALSE\n"
                + "     30 | TRUE\n"
                + "     40 | TRUE\n"
                + "(4 rows)\n"
                + "\n"
                + "!ok\n"
                + "\n");
  }

  @Test public void testUsage() throws Exception {
    final Matcher<String> matcher =
        startsWith("Usage: quidem argument... inFile outFile");
    checkMain(matcher, "--help");
  }

  @Test public void testDbBad() throws Exception {
    checkMain(
        startsWith("Insufficient arguments for --db"),
        "--db",
        "name",
        "jdbc:url");
  }

  @Test public void testDb() throws Exception {
    final File inFile =
        writeFile("!use fm\nselect * from scott.dept;\n!ok\n");
    final File outFile = File.createTempFile("outFile", ".iq");
    final Matcher<String> matcher = equalTo("");
    checkMain(matcher, "--db", "fm", "jdbc:hsqldb:res:scott", "SA", "",
        inFile.getAbsolutePath(), outFile.getAbsolutePath());
    assertThat(toLinux(contents(outFile)),
        equalTo(
            "!use fm\nselect * from scott.dept;\nDEPTNO, DNAME, LOC\n10, ACCOUNTING, NEW YORK\n20, RESEARCH, DALLAS\n30, SALES, CHICAGO\n40, OPERATIONS, BOSTON\n!ok\n"));
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
    checkMain(
        startsWith("Factory class non.existent.ClassName not found"),
        "--factory",
        "non.existent.ClassName");
  }

  @Test public void testFactoryBad2() throws Exception {
    checkMain(
        startsWith("Error instantiating factory class java.lang.String"),
        "--factory",
        "java.lang.String");
  }

  @Test public void testFactory() throws Exception {
    final File inFile =
        writeFile("!use foo\nvalues 1;\n!ok\n");
    final File outFile = File.createTempFile("outFile", ".iq");
    checkMain(equalTo(""), "--factory", FooFactory.class.getName(),
        inFile.getAbsolutePath(), outFile.getAbsolutePath());
    assertThat(toLinux(contents(outFile)),
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

  static Fluent check(String input) {
    return new Fluent(input);
  }

  static void check(String input, Matcher<String> matcher) {
    final StringWriter writer = new StringWriter();
    final Quidem run =
        new Quidem(new BufferedReader(new StringReader(input)), writer);
    run.execute(
        new Quidem.ConnectionFactory() {
          public Connection connect(String name) throws Exception {
            if (name.equals("scott")) {
              Class.forName("org.hsqldb.jdbcDriver");
              return DriverManager.getConnection("jdbc:hsqldb:res:scott", "SA",
                  "");
            }
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

  /** Matcher that applies a regular expression. */
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
        return DriverManager.getConnection("jdbc:hsqldb:res:scott", "SA", "");
      }
      return null;
    }
  }

  /** Fluent class that contains an input string and allows you to test the
   * output in various ways. */
  private static class Fluent {
    private final String input;

    public Fluent(String input) {
      this.input = input;
    }

    public Fluent contains(String string) {
      check(input, containsString(string));
      return this;
    }

    public Fluent outputs(String string) {
      check(input, equalTo(string));
      return this;
    }

    public Fluent matches(String pattern) {
      check(input, new StringMatches(pattern));
      return this;
    }
  }
}

// End QuidemTest.java
