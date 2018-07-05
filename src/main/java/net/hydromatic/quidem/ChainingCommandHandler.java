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

import java.util.List;

/** Command handler that tries several handlers, returning a command
 * from the first that succeeds. */
class ChainingCommandHandler implements CommandHandler {
  private final List<CommandHandler> handlers;

  ChainingCommandHandler(List<CommandHandler> handlers) {
    this.handlers = ImmutableList.copyOf(handlers);
  }

  public Command parseCommand(List<String> lines, List<String> content,
      String line) {
    for (CommandHandler handler : handlers) {
      final Command command = handler.parseCommand(lines, content, line);
      if (command != null) {
        return command;
      }
    }
    return null;
  }
}

// End ChainingCommandHandler.java
