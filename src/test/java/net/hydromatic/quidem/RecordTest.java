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

import net.hydromatic.quidem.record.Config;
import net.hydromatic.quidem.record.Mode;
import net.hydromatic.quidem.record.Recorder;
import net.hydromatic.quidem.record.Recorders;
import net.hydromatic.scott.data.hsqldb.ScottHsqldb;
import net.hydromatic.steelwheels.data.hsqldb.SteelwheelsHsqldb;

import com.google.common.base.Suppliers;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static net.hydromatic.quidem.TestUtils.hasContents;
import static net.hydromatic.quidem.TestUtils.isLines;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests the recorder. */
public class RecordTest {

  static final Supplier<TestUtils.FileFont> TEMP_SUPPLIER =
      Suppliers.memoize(() -> new TestUtils.FileFont("quidem-record-test"));

  static Quidem.ConnectionFactory getScottHsqldb() {
    return ConnectionFactories.simple("scott", ScottHsqldb.URI,
        ScottHsqldb.USER, ScottHsqldb.PASSWORD);
  }

  static Quidem.ConnectionFactory getSteelwheelsHsqldb() {
    return ConnectionFactories.simple("steelwheels", SteelwheelsHsqldb.URI,
        SteelwheelsHsqldb.USER, SteelwheelsHsqldb.PASSWORD);
  }

  /** Records a file containing one query. */
  @Test void testRecord() {
    final File file = TEMP_SUPPLIER.get().file("foo", ".iq");
    checkRecord(file, Mode.RECORD, getScottHsqldb());

    final String[] lines = {
        "# StartTest: empCount",
        "select count(*) from emp;",
        "C1:BIGINT",
        "14",
        "!ok",
        "# EndTest: empCount"
    };
    assertThat(file, hasContents(isLines(lines)));
  }

  @Test void testRecordSeveral() {
    final File file = TEMP_SUPPLIER.get().file("foo2", ".iq");
    final Quidem.ConnectionFactory connectionFactory =
        ConnectionFactories.chain(getScottHsqldb(), getSteelwheelsHsqldb());
    final String[] lines = {
        "# StartTest: empCount",
        "select count(*) from emp;",
        "C1:BIGINT",
        "14",
        "!ok",
        "# EndTest: empCount",
        "# StartTest: managers",
        "select *",
        "from emp",
        "where job in ('MANAGER', 'PRESIDENT')",
        "order by empno;",
        "EMPNO:SMALLINT,ENAME:VARCHAR,JOB:VARCHAR,MGR:SMALLINT,HIREDATE:DATE,SAL:DECIMAL,COMM:DECIMAL,DEPTNO:TINYINT",
        "7566,JONES,MANAGER,7839,1981-02-04,2975.00,,20",
        "7698,BLAKE,MANAGER,7839,1981-01-05,2850.00,,30",
        "7782,CLARK,MANAGER,7839,1981-06-09,2450.00,,10",
        "7839,KING,PRESIDENT,,1981-11-17,5000.00,,10",
        "!ok",
        "# EndTest: managers",
        "# StartTest: productCount",
        "select count(*) as c",
        "from \"products\";",
        "C:BIGINT",
        "110",
        "!ok",
        "# EndTest: productCount"
    };
    checkRecordSeveral(file, Mode.PASS_THROUGH, connectionFactory);
    assertThat(file.exists(), is(false));
    checkRecordSeveral(file, Mode.RECORD, connectionFactory);
    assertThat(file.exists(), is(true));
    assertThat(file, hasContents(isLines(lines)));
    checkRecordSeveral(file, Mode.PLAY, connectionFactory);
  }

  private static void checkRecord(File file, Mode mode,
      Quidem.ConnectionFactory connectionFactory) {
    Config config = Recorders.config()
        .withFile(file)
        .withMode(mode)
        .withConnectionFactory(connectionFactory);
    try (Recorder recorder = Recorders.create(config)) {
      recorder.executeQuery("scott", "empCount", "select count(*) from emp",
          isInt(14));
    }
  }

  private static void checkRecordSeveral(File file, Mode mode,
      Quidem.ConnectionFactory connectionFactory) {
    Config config = Recorders.config()
        .withFile(file)
        .withMode(mode)
        .withConnectionFactory(connectionFactory);
    try (Recorder recorder = Recorders.create(config)) {
      recorder.executeQuery("scott", "empCount", "select count(*) from emp",
          isInt(14));
      recorder.executeQuery("scott", "managers",
          "select *\n"
              + "from emp\n"
              + "where job in ('MANAGER', 'PRESIDENT')\n"
              + "order by empno",
          result -> {
            try {
              final ResultSetMetaData metaData = result.getMetaData();
              assertThat(metaData.getColumnCount(), is(8));
              assertThat(metaData.getColumnName(1), is("EMPNO"));
              assertThat(metaData.getColumnTypeName(1), is("SMALLINT"));
              assertThat(metaData.getColumnType(1), is(Types.SMALLINT));
              assertThat(metaData.getColumnName(2), is("ENAME"));
              assertThat(metaData.getColumnTypeName(2), is("VARCHAR"));
              assertThat(metaData.getColumnType(2), is(Types.VARCHAR));
              assertThat(metaData.getColumnName(3), is("JOB"));
              assertThat(metaData.getColumnTypeName(3), is("VARCHAR"));
              assertThat(metaData.getColumnType(3), is(Types.VARCHAR));
              assertThat(metaData.getColumnName(4), is("MGR"));
              assertThat(metaData.getColumnTypeName(4), is("SMALLINT"));
              assertThat(metaData.getColumnType(4), is(Types.SMALLINT));
              assertThat(metaData.getColumnName(5), is("HIREDATE"));
              assertThat(metaData.getColumnTypeName(5), is("DATE"));
              assertThat(metaData.getColumnType(5), is(Types.DATE));
              assertThat(metaData.getColumnName(6), is("SAL"));
              assertThat(metaData.getColumnTypeName(6), is("DECIMAL"));
              assertThat(metaData.getColumnType(6), is(Types.DECIMAL));
              assertThat(metaData.getColumnName(7), is("COMM"));
              assertThat(metaData.getColumnTypeName(7), is("DECIMAL"));
              assertThat(metaData.getColumnType(7), is(Types.DECIMAL));
              assertThat(metaData.getColumnName(8), is("DEPTNO"));
              assertThat(metaData.getColumnTypeName(8), is("TINYINT"));
              assertThat(metaData.getColumnType(8), is(Types.TINYINT));
              assertThat(result.next(), is(true));
              assertThat(result.getInt(1), is(7566)); // EMPNO
              assertThat(result.getString(2), is("JONES")); // ENAME
              assertThat(result.getDate(5),
                  hasToString("1981-02-04")); // HIREDATE
              assertThat(result.getBigDecimal(6),
                  hasToString("2975.00")); // SAL
              assertThat(result.getBigDecimal(7), nullValue()); // COMM
            } catch (SQLException e) {
              fail(e);
            }
          });
      recorder.executeQuery("steelwheels", "productCount",
          "select count(*) as c\n"
              + "from \"products\"",
          isInt(110));
    }
  }

  /** Tests what happens if there is no file. */
  @Test void testNoFile() {
    for (Mode mode : Mode.values()) {
      switch (mode) {
      case PLAY:
      case RECORD:
        // PLAY and RECORD require a file
        try (Recorder recorder =
                 Recorders.create(Recorders.config().withMode(mode))) {
          fail("expected error, got " + recorder);
        } catch (IllegalStateException e) {
          assertThat(e.getMessage(),
              is("mode '" + mode + "' requires a file"));
        }
        break;

      case PASS_THROUGH:
        // PASS_THROUGH does not require a file
        try (Recorder recorder =
                 Recorders.create(Recorders.config().withMode(mode))) {
          assertThat(recorder, notNullValue());
        }
        break;

      default:
        throw new AssertionError(mode);
      }
    }
  }

  /** Creates a recording with two queries, tries to execute a query that is
   * missing. */
  @Test void testRecord2() {
    final File file = TEMP_SUPPLIER.get().file("testRecord2", ".iq");
    final String[] lines = {
        "# StartTest: one",
        "select 1;",
        "C1:BIGINT",
        "1",
        "!ok",
        "# EndTest: one",
        "# StartTest: three",
        "select 3;",
        "C1:BIGINT",
        "3",
        "!ok",
        "# EndTest: three"
    };
    try (FileWriter w = new FileWriter(file);
         PrintWriter pw = new PrintWriter(w)) {
      for (String line : lines) {
        pw.println(line);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Config config = Recorders.config()
        .withFile(file)
        .withMode(Mode.PLAY)
        .withConnectionFactory(getScottHsqldb());
    try (Recorder recorder = Recorders.create(config)) {
      recorder.executeQuery("scott", "one", "select 1", isInt(1));
      recorder.executeQuery("scott", "one", "select 3", isInt(3));
      try {
        recorder.executeQuery("scott", "one", "select 2",
            resultSet -> fail("should not be called"));
        fail("should not reach this point");
      } catch (IllegalArgumentException e) {
        assertThat(e.getMessage(), is("sql [select 2] is not in recording"));
      }
    }
  }

  /** Returns a consumer that checks that a {@link java.sql.ResultSet} has
   * one integer column, one row, and the value in that row is {@code value}. */
  private static Consumer<ResultSet> isInt(int value) {
    return result -> {
      try {
        assertThat(result.getMetaData().getColumnCount(), is(1));
        assertThat(result.next(), is(true));
        assertThat(result.getInt(1), is(value));
        assertThat(result.next(), is(false));
      } catch (SQLException e) {
        fail(e);
      }
    };
  }

  void simpleQuery(Recorder recorder) {
    recorder.executeQuery("scott", "one", "select 1", resultSet -> { });
  }

}

// End RecordTest.java
