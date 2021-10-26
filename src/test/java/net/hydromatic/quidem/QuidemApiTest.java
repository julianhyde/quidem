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

import java.io.StringReader;
import java.io.StringWriter;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link Quidem}'s API.
 */
public class QuidemApiTest {
  /** Unit test for {@link Quidem#isProbablyDeterministic(String)} and in
   * particular for
   * <a href="https://github.com/julianhyde/quidem/issues/7">[QUIDEM-7]
   * Don't be fooled by ORDER BY inside windowed aggregate</a>. */
  @Test void testDeterministic() {
    final Quidem run =
        new Quidem(new StringReader(""), new StringWriter());
    assertThat(run.isProbablyDeterministic("select * from emp"), is(false));
    assertThat(run.isProbablyDeterministic("select * from emp order by deptno"),
        is(true));
    assertThat(
        run.isProbablyDeterministic("select empno,\n"
            + " sum(sal) over (partition by deptno order by empid)\n"
            + "from emp order by deptno"),
        is(true));
    assertThat(
        run.isProbablyDeterministic("select empno,\n"
            + " sum(sal) over (partition by deptno order by empid)\n"
            + "from emp"),
        is(false));
    assertThat(
        run.isProbablyDeterministic("select empno\n"
            + "from emp order by (deptno + 10) / 2 desc"),
        is(true));
  }
}

// End QuidemApiTest.java
