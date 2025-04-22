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

import com.google.common.base.Throwables;
import java.io.FilterWriter;
import java.io.IOException;
import java.io.Writer;

/** Writer that accepts only a given amount of text. */
class LimitWriter extends FilterWriter {
  private int length;
  private final int maxLength;

  protected LimitWriter(Writer w, int maxLength) {
    super(w);
    this.maxLength = maxLength;
  }

  private int length() {
    return length;
  }

  @Override
  public void write(int c) throws IOException {
    final int newLength = length() + 1;
    if (newLength <= maxLength) {
      super.write(c);
      ++length;
    }
  }

  @Override
  public void write(char[] cbuf, int off, int len) throws IOException {
    if (length + len <= maxLength) {
      // enough room for whole string
      super.write(cbuf, off, len);
      length += len;
    } else if (length < maxLength) {
      // enough room for part of string
      super.write(cbuf, off, maxLength - length);
      length = maxLength;
    }
  }

  @Override
  public void write(String str, int off, int len) throws IOException {
    if (length + len <= maxLength) {
      // enough room for whole string
      super.write(str, off, len);
      length += len;
    } else if (length < maxLength) {
      // enough room for part of string
      super.write(str, off, maxLength - length);
      length = maxLength;
    }
  }

  /** Appends a message if the limit has been reached or exceeded. */
  public void ellipsis(String message) {
    if (length >= maxLength) {
      try {
        out.write(message);
        out.flush();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}

// End LimitWriter.java
