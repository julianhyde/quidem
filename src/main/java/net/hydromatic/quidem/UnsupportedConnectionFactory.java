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

import java.sql.Connection;

/** Connection factory that says all databases are unknown,
 * and returns null when asked for a reference connection. */
class UnsupportedConnectionFactory
    implements Quidem.ConnectionFactory {
  public Connection connect(String name, boolean reference) {
    if (reference) {
      return null;
    }
    throw new RuntimeException("Unknown database: " + name);
  }
}

// End UnsupportedConnectionFactory.java
