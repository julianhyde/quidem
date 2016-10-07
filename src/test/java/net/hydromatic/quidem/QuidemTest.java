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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import org.hamcrest.Matcher;
import org.hamcrest.core.SubstringMatcher;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

/**
 * Script-based tests for {@link Quidem}.
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

  @Test public void testErrorTruncated() {
    check(
        "!use scott\n"
        + "select blah from blah;\n"
        + "!ok\n"
        + "\n")
        .limit(10)
        .contains(
            "!use scott\n"
            + "select blah from blah;\n"
            + "java.sql.S (stack truncated)");
  }

  @Test public void testErrorNotTruncated() {
    check(
        "!use scott\n"
        + "select blah from blah;\n"
        + "!ok\n"
        + "\n")
        .limit(1000)
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

  /** The error does not even need to be a fully line. */
  @Test public void testExpectErrorPartialLine() {
    final String input = "!use scott\n"
        + "select blah from blah;\n"
        + "lacks privilege\n"
        + "!error\n"
        + "\n";
    final String expected = "(?s)!use scott\n"
        + "select blah from blah;\n"
        + "lacks privilege\n"
        + "!error\n"
        + "\n";
    check(input).matches(expected);
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

  /** Test case for
   * <a href="https://github.com/julianhyde/quidem/issues/8">[QUIDEM-8]
   * Allow variable in 'if'</a>. */
  @Test public void testIfVariable() {
    check("!use scott\n"
        + "!if (affirmative) {\n"
        + "values (1), (2);\n"
        + "anything\n"
        + "you like\n"
        + "!ok\n"
        + "!}\n"
        + "!if (negative) {\n"
        + "values (1), (2);\n"
        + "anything\n"
        + "you like\n"
        + "!ok\n"
        + "!}\n"
        + "!if (unset) {\n"
        + "values (1), (2);\n"
        + "anything\n"
        + "you like\n"
        + "!ok\n"
        + "!}\n"
        + "\n")
        .contains("!use scott\n"
            + "!if (affirmative) {\n"
            + "values (1), (2);\n"
            + "C1\n"
            + "1\n"
            + "2\n"
            + "!ok\n"
            + "!}\n"
            + "!if (negative) {\n"
            + "values (1), (2);\n"
            + "anything\n"
            + "you like\n"
            + "!ok\n"
            + "!}\n"
            + "!if (unset) {\n"
            + "values (1), (2);\n"
            + "anything\n"
            + "you like\n"
            + "!ok\n"
            + "!}\n"
            + "\n");
  }

  /** Test case for
   * <a href="https://github.com/julianhyde/quidem/issues/11">[QUIDEM-11]
   * Nested variables</a>. */
  @Test public void testIfVariableNested() {
    final String input = "!use scott\n"
        + "!if (sun.self.self.hot) {\n"
        + "values (1), (2);\n"
        + "anything\n"
        + "you like\n"
        + "!ok\n"
        + "!}\n"
        + "!if (sun.cold) {\n"
        + "values (1), (2);\n"
        + "anything\n"
        + "you like\n"
        + "!ok\n"
        + "!}\n"
        + "!if (sun.unset.foo.baz) {\n"
        + "values (1), (2);\n"
        + "anything\n"
        + "you like\n"
        + "!ok\n"
        + "!}\n"
        + "\n";
    final String output = "!use scott\n"
        + "!if (sun.self.self.hot) {\n"
        + "values (1), (2);\n"
        + "C1\n"
        + "1\n"
        + "2\n"
        + "!ok\n"
        + "!}\n"
        + "!if (sun.cold) {\n"
        + "values (1), (2);\n"
        + "anything\n"
        + "you like\n"
        + "!ok\n"
        + "!}\n"
        + "!if (sun.unset.foo.baz) {\n"
        + "values (1), (2);\n"
        + "anything\n"
        + "you like\n"
        + "!ok\n"
        + "!}\n"
        + "\n";
    check(input).contains(output);
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

  @Ignore // TODO: fixme
  @Test public void testSqlIfFalsePlan() {
    check(
        "!use foodmart\n"
        + "values 1;\n"
        + "!if (false) {\n"
        + "anything\n"
        + "you like\n"
        + "!ok\n"
        + "!}\n"
        + "something\n"
        + "!plan\n"
        + "\n",
        ImmutableList.<Function<Quidem, Quidem>>of(),
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

  /** Tests the '!update' command against INSERT and DELETE statements,
   * and also checks an intervening '!plan'. */
  @Test public void testUpdate() {
    final String input = "!use scott\n"
        + "insert into scott.dept values (50, 'DEV', 'SAN DIEGO');\n"
        + "!update\n"
        + "!plan\n"
        + "\n";
    final String output = "!use scott\n"
        + "insert into scott.dept values (50, 'DEV', 'SAN DIEGO');\n"
        + "(1 row modified)\n"
        + "\n"
        + "!update\n"
        + "INSERT VALUES[\n"
        + "\n"
        + "TABLE[DEPT]\n"
        + "PARAMETERS=[]\n"
        + "SUBQUERIES[]]\n"
        + "!plan\n"
        + "\n";
    check(input).contains(output);

    // remove the row
    final String input2 = "!use scott\n"
        + "delete from scott.dept where deptno = 50;\n"
        + "!update\n"
        + "\n";
    final String output2 = "!use scott\n"
        + "delete from scott.dept where deptno = 50;\n"
        + "(1 row modified)\n"
        + "\n"
        + "!update\n"
        + "\n";
    check(input2).contains(output2);

    // no row to remove
    final String output3 = "!use scott\n"
        + "delete from scott.dept where deptno = 50;\n"
        + "(0 rows modified)\n"
        + "\n"
        + "!update\n"
        + "\n";
    check(input2).contains(output3);
  }

  /** Tests the '!type' command. */
  @Test public void testType() {
    final String input = "!use scott\n"
        + "select empno, deptno, sal from scott.emp where empno < 7400;\n"
        + "!ok\n"
        + "!type\n"
        + "\n";
    final String output = "!use scott\n"
        + "select empno, deptno, sal from scott.emp where empno < 7400;\n"
        + "EMPNO, DEPTNO, SAL\n"
        + "7369, 20, 800.00\n"
        + "!ok\n"
        + "EMPNO SMALLINT(16) NOT NULL\n"
        + "DEPTNO TINYINT(8)\n"
        + "SAL DECIMAL(7, 2)\n"
        + "!type\n"
        + "\n";
    check(input).contains(output);
  }

  /** Tests the '!verify' command. */
  @Test public void testVerify() {
    final String input = "!use empty\n"
        + "select * from INFORMATION_SCHEMA.TABLES;\n"
        + "!verify\n"
        + "\n";
    final String output = "!use empty\n"
        + "select * from INFORMATION_SCHEMA.TABLES;\n"
        + "!verify\n"
        + "\n";
    check(input).contains(output);
  }

  /** Tests the '!verify' command where the reference database produces
   * different output. */
  @Test public void testVerifyDiff() {
    // Database "empty" sorts nulls first;
    // its reference database sorts nulls last.
    final String input = "!use empty\n"
        + "select * from (values (1,null),(2,'a')) order by 2;\n"
        + "!verify\n"
        + "\n";
    final String output = "!use empty\n"
        + "select * from (values (1,null),(2,'a')) order by 2;\n"
        + "!verify\n"
        + "Error while executing command VerifyCommand [sql: select * from (values (1,null),(2,'a')) order by 2\n"
        + "]\n"
        + "java.lang.IllegalArgumentException: Reference query returned different results.\n"
        + "expected:\n"
        + "C1, C2\n"
        + "2, a\n"
        + "1, null\n"
        + "actual:\n"
        + "C1, C2\n"
        + "1, null\n"
        + "2, a\n";
    check(input).contains(output);
  }

  /** Tests the '!verify' command with a database that has no reference. */
  @Test public void testVerifyNoReference() {
    final String input = "!use scott\n"
        + "select * from scott.emp;\n"
        + "!verify\n"
        + "\n";
    final String output = "!use scott\n"
        + "select * from scott.emp;\n"
        + "!verify\n"
        + "Error while executing command VerifyCommand [sql: select * from scott.emp\n"
        + "]\n"
        + "java.lang.IllegalArgumentException: no reference connection\n";
    check(input).contains(output);
  }

  @Test public void testUsage() throws Exception {
    final Matcher<String> matcher =
        startsWith("Usage: quidem argument... inFile outFile");
    checkMain(matcher, 0, "--help");
  }

  @Test public void testDbBad() throws Exception {
    checkMain(startsWith("Insufficient arguments for --db"), 1,
        "--db", "name", "jdbc:url");
  }

  @Test public void testDb() throws Exception {
    final File inFile =
        writeFile("!use fm\nselect * from scott.dept;\n!ok\n");
    final File outFile = File.createTempFile("outFile", ".iq");
    final Matcher<String> matcher = equalTo("");
    checkMain(matcher, 0, "--db", "fm", "jdbc:hsqldb:res:scott", "SA", "",
        inFile.getAbsolutePath(), outFile.getAbsolutePath());
    assertThat(toLinux(contents(outFile)),
        equalTo("!use fm\n"
            + "select * from scott.dept;\n"
            + "DEPTNO, DNAME, LOC\n"
            + "10, ACCOUNTING, NEW YORK\n"
            + "20, RESEARCH, DALLAS\n"
            + "30, SALES, CHICAGO\n"
            + "40, OPERATIONS, BOSTON\n"
            + "!ok\n"));
    //noinspection ResultOfMethodCallIgnored
    inFile.delete();
    //noinspection ResultOfMethodCallIgnored
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
    checkMain(startsWith("Factory class non.existent.ClassName not found"), 1,
        "--factory", "non.existent.ClassName");
  }

  @Test public void testFactoryBad2() throws Exception {
    checkMain(startsWith("Error instantiating factory class java.lang.String"),
        1, "--factory", "java.lang.String");
  }

  @Test public void testFactory() throws Exception {
    final File inFile =
        writeFile("!use foo\nvalues 1;\n!ok\n");
    final File outFile = File.createTempFile("outFile", ".iq");
    checkMain(equalTo(""), 0, "--factory", FooFactory.class.getName(),
        inFile.getAbsolutePath(), outFile.getAbsolutePath());
    assertThat(toLinux(contents(outFile)),
        equalTo("!use foo\nvalues 1;\nC1\n1\n!ok\n"));
    //noinspection ResultOfMethodCallIgnored
    inFile.delete();
    //noinspection ResultOfMethodCallIgnored
    outFile.delete();
  }

  @Test public void testVar() throws Exception {
    final File inFile =
        writeFile("!if (myVar) {\nblah;\n!ok\n!}\n");
    final File outFile = File.createTempFile("outFile", ".iq");
    final Matcher<String> matcher = equalTo("");
    checkMain(matcher, 0, "--var", "myVar", "true",
        inFile.getAbsolutePath(), outFile.getAbsolutePath());
    assertThat(toLinux(contents(outFile)),
        startsWith("!if (myVar) {\n"
            + "blah;\n"
            + "!ok\n"
            + "Error while executing command OkCommand [sql: blah\n"
            + "]\n"
            + "java.lang.RuntimeException: no connection\n"));
    //noinspection ResultOfMethodCallIgnored
    inFile.delete();
    //noinspection ResultOfMethodCallIgnored
    outFile.delete();
  }

  @Test public void testVarFalse() throws Exception {
    final File inFile =
        writeFile("!if (myVar) {\nblah;\n!ok\n!}\n");
    final File outFile = File.createTempFile("outFile", ".iq");
    final Matcher<String> matcher = equalTo("");
    checkMain(matcher, 0, "--var", "myVar", "false",
        inFile.getAbsolutePath(), outFile.getAbsolutePath());
    assertThat(toLinux(contents(outFile)),
        equalTo("!if (myVar) {\nblah;\n!ok\n!}\n"));
    //noinspection ResultOfMethodCallIgnored
    inFile.delete();
    //noinspection ResultOfMethodCallIgnored
    outFile.delete();
  }

  @Test public void testSetBoolean() {
    final String input = "!use scott\n"
        + "!show foo\n"
        + "!set foo true\n"
        + "!show foo\n"
        + "!if (foo) {\n"
        + "values 1;\n"
        + "!ok;\n"
        + "!}\n"
        + "!set foo false\n"
        + "!if (foo) {\n"
        + "values 2;\n"
        + "!ok;\n"
        + "!}\n"
        + "!push foo true\n"
        + "!push foo false\n"
        + "!push foo true\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "!show foo\n";
    final String output = "!use scott\n"
        + "foo null\n"
        + "!show foo\n"
        + "!set foo true\n"
        + "foo true\n"
        + "!show foo\n"
        + "!if (foo) {\n"
        + "values 1;\n"
        + "C1\n"
        + "1\n"
        + "!ok;\n"
        + "!}\n"
        + "!set foo false\n"
        + "!if (foo) {\n"
        + "values 2;\n"
        + "!ok;\n"
        + "!}\n"
        + "!push foo true\n"
        + "!push foo false\n"
        + "!push foo true\n"
        + "foo true\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "foo false\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "foo true\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "foo false\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "foo null\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "Cannot pop foo: stack is empty\n"
        + "foo null\n"
        + "!show foo\n";
    check(input).contains(output);
  }

  @Test public void testSetInteger() {
    final String input = "!use scott\n"
        + "!show foo\n"
        + "!set foo -123\n"
        + "!show foo\n"
        + "!push foo 345\n"
        + "!push bar 0\n"
        + "!push foo hello\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "!show foo\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "!show foo\n";
    final String output = "!use scott\n"
        + "foo null\n"
        + "!show foo\n"
        + "!set foo -123\n"
        + "foo -123\n"
        + "!show foo\n"
        + "!push foo 345\n"
        + "!push bar 0\n"
        + "!push foo hello\n"
        + "foo hello\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "foo 345\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "foo -123\n"
        + "!show foo\n"
        + "foo -123\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "foo null\n"
        + "!show foo\n";
    check(input).contains(output);
  }

  /** Tests that the {@link net.hydromatic.quidem.Quidem.PropertyHandler} is
   * called whenever there is a set, push or pop. */
  @Test public void testPropertyHandler() {
    final String input = "!use scott\n"
        + "!show foo\n"
        + "!set foo -123\n"
        + "!show foo\n"
        + "!push foo 345\n"
        + "!push bar 0\n"
        + "!push foo hello\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "!show foo\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "!show foo\n";
    final String output = "!use scott\n"
        + "foo null\n"
        + "!show foo\n"
        + "!set foo -123\n"
        + "foo -123\n"
        + "!show foo\n"
        + "!push foo 345\n"
        + "!push bar 0\n"
        + "!push foo hello\n"
        + "foo hello\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "foo 345\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "foo -123\n"
        + "!show foo\n"
        + "foo -123\n"
        + "!show foo\n"
        + "!pop foo\n"
        + "foo null\n"
        + "!show foo\n";
    final StringBuilder b = new StringBuilder();
    final Quidem.PropertyHandler propertyHandler =
        new Quidem.PropertyHandler() {
          public void onSet(String propertyName, Object value) {
            b.append(propertyName).append('=').append(value).append('\n');
          }
        };
    check(input).withPropertyHandler(propertyHandler).contains(output);
    final String propertyEvents = "foo=-123\n"
        + "foo=345\n"
        + "bar=0\n"
        + "foo=hello\n"
        + "foo=345\n"
        + "foo=-123\n"
        + "foo=null\n";
    assertThat(b.toString(), is(propertyEvents));
  }

  @Test public void testLimitWriter() throws IOException {
    final StringWriter w = new StringWriter();
    LimitWriter limitWriter = new LimitWriter(w, 6);
    limitWriter.append("abcdefghiklmnopq");
    assertThat(w.toString(), equalTo("abcdef"));

    // We already exceeded limit. Clearing the backing buffer does not help.
    w.getBuffer().setLength(0);
    limitWriter.append("xxxxx");
    limitWriter.append("yyyyy");
    assertThat(w.toString(), equalTo(""));

    // Create a new writer to reset the count.
    limitWriter = new LimitWriter(w, 6);
    w.getBuffer().setLength(0);
    limitWriter.append("xxxxx");
    limitWriter.append("yyyyy");
    assertThat(w.toString(), equalTo("xxxxxy"));

    limitWriter = new LimitWriter(w, 6);
    w.getBuffer().setLength(0);
    limitWriter.append("xxx");
    limitWriter.append('y');
    limitWriter.append('y');
    limitWriter.append('y');
    limitWriter.append('y');
    limitWriter.append('y');
    limitWriter.append('y');
    limitWriter.append('y');
    limitWriter.append("");
    limitWriter.append('z');
    limitWriter.append("zzzzzzzzz");
    assertThat(w.toString(), equalTo("xxxyyy"));

    limitWriter = new LimitWriter(w, 6);
    w.getBuffer().setLength(1);
    assertThat(w.toString(), equalTo("x"));
    w.getBuffer().setLength(2);
    assertThat(w.toString(), equalTo("x\0"));
    limitWriter.write(new char[]{'a', 'a', 'a', 'a', 'a'}, 0, 3);
    assertThat(w.toString(), equalTo("x\0aaa"));
  }

  private void checkMain(Matcher<String> matcher, int expectedCode,
      String... args) throws Exception {
    final StringWriter sw = new StringWriter();
    final PrintWriter pw = new PrintWriter(sw);
    final int code = Launcher.main2(pw, pw, Arrays.asList(args));
    pw.close();
    assertThat(sw.toString(), matcher);
    assertThat(code, equalTo(expectedCode));
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

  static void check(String input, List<Function<Quidem, Quidem>> transformList,
      Matcher<String> matcher) {
    final StringWriter writer = new StringWriter();
    final Function<String, Object> env = new Function<String, Object>() {
      public Object apply(String input) {
        assert input != null;
        return input.equals("affirmative") ? Boolean.TRUE
            : input.equals("negative") ? Boolean.FALSE
            : input.equals("sun")
            ? new Function<String, Object>() {
              public Object apply(String input) {
                assert input != null;
                return input.equals("hot") ? Boolean.TRUE
                    : input.equals("cold") ? Boolean.FALSE
                    : input.equals("self") ? this
                    : null;
              }
            }
            : null;
      }
    };
    final Quidem.ConnectionFactory connectionFactory =
        new Quidem.ConnectionFactory() {
          public Connection connect(String name, boolean reference)
              throws Exception {
            if (name.equals("scott")) {
              Class.forName("org.hsqldb.jdbcDriver");
              final Connection connection =
                  DriverManager.getConnection("jdbc:hsqldb:res:scott", "SA",
                      "");
              if (reference) {
                return null; // no reference connection available for empty
              }
              return connection;
            }
            if (name.startsWith("empty")) {
              Class.forName("org.hsqldb.jdbcDriver");
              if (reference) {
                name += "_ref";
              }
              final Connection connection =
                  DriverManager.getConnection("jdbc:hsqldb:mem:" + name, "",
                      "");
              if (reference) {
                final Statement statement = connection.createStatement();
                statement.executeQuery("SET DATABASE SQL NULLS FIRST FALSE");
                statement.close();
              }
              return connection;
            }
            throw new RuntimeException("unknown connection '" + name + "'");
          }
        };
    Quidem run =
        new Quidem(new StringReader(input), writer, env, connectionFactory);
    for (Function<Quidem, Quidem> transform : transformList) {
      run = transform.apply(run);
    }
    run.execute();
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
    public Connection connect(String name, boolean reference) throws Exception {
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
    private final List<Function<Quidem, Quidem>> transformList;

    public Fluent(String input) {
      this(input, ImmutableList.<Function<Quidem, Quidem>>of());
    }

    public Fluent(String input, List<Function<Quidem, Quidem>> transformList) {
      this.input = input;
      this.transformList = transformList;
    }

    public Fluent contains(String string) {
      check(input, transformList, containsString(string));
      return this;
    }

    public Fluent outputs(String string) {
      check(input, transformList, equalTo(string));
      return this;
    }

    public Fluent matches(String pattern) {
      check(input, transformList, new StringMatches(pattern));
      return this;
    }

    public Fluent limit(final int i) {
      return new Fluent(input,
          plus(transformList,
              new Function<Quidem, Quidem>() {
                public Quidem apply(Quidem quidem) {
                  quidem.setStackLimit(i);
                  return quidem;
                }
              }));
    }

    private static <E> List<E> plus(List<E> list, E element) {
      return ImmutableList.<E>builder().addAll(list).add(element).build();
    }

    public Fluent withPropertyHandler(
        final Quidem.PropertyHandler propertyHandler) {
      return new Fluent(input,
          plus(transformList,
              new Function<Quidem, Quidem>() {
                public Quidem apply(Quidem quidem) {
                  return quidem.withPropertyHandler(propertyHandler);
                }
              }));
    }
  }
}

// End QuidemTest.java
