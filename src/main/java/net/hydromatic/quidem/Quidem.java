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
import com.google.common.base.Preconditions;
import com.google.common.collect.*;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * Runs a SQL script.
 */
public class Quidem {
  private static final Ordering<String[]> ORDERING =
      Ordering.natural().nullsLast().lexicographical().onResultOf(
          new Function<String[], Iterable<Comparable>>() {
            public Iterable<Comparable> apply(String[] input) {
              return Arrays.<Comparable>asList(input);
            }
          });

  public static final boolean DEBUG =
    "true".equals(System.getProperties().getProperty("quidem.debug"));

  private static final String[] USAGE_LINES = {
    "Usage: quidem argument... inFile outFile",
    "",
    "Arguments:",
    "  --help",
    "           Print usage",
    "  --db name url user password",
    "           Add a database to the connection factory",
    "  --factory className",
    "           Define a factory class"
  };

  private BufferedReader reader;
  private Writer writer;
  private PrintWriter printWriter;
  private final Map<Property, Object> map = new HashMap<Property, Object>();
  /** Result set from SQL statement just executed. */
  private ResultSet resultSet;
  /** Whether to sort result set before printing. */
  private boolean sort;
  private SQLException resultSetException;
  private final List<String> lines = new ArrayList<String>();
  private String pushedLine;
  private final StringBuilder buf = new StringBuilder();
  private Connection connection;
  private ConnectionFactory connectionFactory;
  private boolean execute = true;
  private boolean skip = false;

  public Quidem(BufferedReader reader, Writer writer) {
    this.reader = reader;
    this.writer = writer;
    this.map.put(Property.OUTPUTFORMAT, OutputFormat.CSV);
  }

  public static void main(String[] args) {
    final PrintWriter pw = new PrintWriter(System.out);
    try {
      main2(Arrays.asList(args), pw);
      pw.flush();
    } catch (Throwable e) {
      pw.flush();
      e.printStackTrace();
      System.exit(1);
    }
  }

  public static void main2(List<String> args, PrintWriter out)
      throws Exception {
    final List<ConnectionFactory> factories = Lists.newArrayList();
    int i;
    for (i = 0; i < args.size();) {
      String arg = args.get(i);
      if (arg.equals("--help")) {
        usage(out, null);
        return;
      }
      if (arg.equals("--db")) {
        if (i + 4 >= args.size()) {
          usage(out, "Insufficient arguments for --db");
          return;
        }
        final String name = args.get(i + 1);
        final String url = args.get(i + 2);
        final String user = args.get(i + 3);
        final String password = args.get(i + 4);
        factories.add(new SimpleConnectionFactory(name, url, user, password));
        i += 5;
        continue;
      }
      if (arg.equals("--factory")) {
        if (i + 1 >= args.size()) {
          usage(out, "Insufficient arguments for --factory");
        }
        final String className = args.get(i + 1);
        final Class<?> factoryClass;
        try {
          factoryClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
          usage(out, "Factory class " + className + " not found");
          return;
        }
        ConnectionFactory factory;
        try {
          factory = (ConnectionFactory) factoryClass.newInstance();
        } catch (InstantiationException e) {
          usage(out, "Error instantiating factory class " + className);
          return;
        } catch (IllegalAccessException e) {
          usage(out, "Error instantiating factory class " + className);
          return;
        } catch (ClassCastException e) {
          usage(out, "Error instantiating factory class " + className);
          return;
        }
        factories.add(factory);
        i += 2;
        continue;
      }
      break;
    }
    if (i + 2 > args.size()) {
      usage(out, "Insufficient arguments: need inFile and outFile");
      return;
    }
    final File inFile = new File(args.get(i));
    final File outFile = new File(args.get(i + 1));
    final Reader reader;
    try {
      reader = new LineNumberReader(new FileReader(inFile));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Error opening input " + inFile, e);
    }
    final Writer writer;
    try {
      writer = new FileWriter(outFile);
    } catch (IOException e) {
      throw new RuntimeException("Error opening output " + outFile, e);
    }
    factories.add(new UnsupportedConnectionFactory());
    final Quidem quidem = new Quidem(new BufferedReader(reader), writer);

    quidem.execute(new ChainingConnectionFactory(factories));
    reader.close();
    writer.close();
  }

  private static void usage(PrintWriter out, String error) {
    if (error != null) {
      out.println(error);
      out.println();
    }
    for (String line : USAGE_LINES) {
      out.println(line);
    }
  }

  private void close() throws SQLException {
    if (connection != null) {
      Connection c = connection;
      connection = null;
      c.close();
    }
  }

  public void execute(ConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
    this.printWriter = new PrintWriter(writer);
    try {
      Command command = new Parser().parse();
      try {
        command.execute(execute);
        close();
      } catch (Exception e) {
        throw new RuntimeException(
            "Error while executing command " + command, e);
      } catch (AssertionError e) {
        throw new RuntimeException(
            "Error while executing command " + command, e);
      }
    } finally {
      printWriter.flush();
      try {
        close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  Command of(List<Command> commands) {
    return commands.size() == 1
        ? commands.get(0)
        : new CompositeCommand(ImmutableList.copyOf(commands));
  }

  private static String pad(String s, int width, boolean right) {
    if (s == null) {
      s = "";
    }
    final int x = width - s.length();
    if (x <= 0) {
      return s;
    }
    final StringBuilder buf = new StringBuilder();
    if (right) {
      buf.append(chars(' ', x)).append(s);
    } else {
      buf.append(s).append(chars(' ', x));
    }
    return buf.toString();
  }

  private static CharSequence chars(final char c, final int length) {
    return new CharSequence() {
      @Override public String toString() {
        final char[] chars = new char[length];
        Arrays.fill(chars, c);
        return new String(chars);
      }

      public int length() {
        return length;
      }

      public char charAt(int index) {
        return c;
      }

      public CharSequence subSequence(int start, int end) {
        return Quidem.chars(c, end - start);
      }
    };
  }

  /** Connection factory that recognizes a single name. */
  private static class SimpleConnectionFactory implements ConnectionFactory {
    private final String name;
    private final String url;
    private final String user;
    private final String password;

    public SimpleConnectionFactory(String name, String url, String user,
        String password) {
      this.name = name;
      this.url = url;
      this.user = user;
      this.password = password;
    }

    @Override public Connection connect(String name) throws Exception {
      if (name.equals(this.name)) {
        return DriverManager.getConnection(url, user, password);
      }
      return null;
    }
  }

  /** Connection factory that says all databases are unknown. */
  private static class UnsupportedConnectionFactory
      implements ConnectionFactory {
    public Connection connect(String name) {
      throw new RuntimeException("Unknown database: " + name);
    }
  }

  /** Connection factory that tries several factories, returning a connection
   * from the first that is able to connect. */
  private static class ChainingConnectionFactory implements ConnectionFactory {
    private final List<ConnectionFactory> factories;

    public ChainingConnectionFactory(List<ConnectionFactory> factories) {
      this.factories = ImmutableList.copyOf(factories);
    }

    @Override public Connection connect(String name) throws Exception {
      for (ConnectionFactory factory : factories) {
        Connection c = factory.connect(name);
        if (c != null) {
          return c;
        }
      }
      return null;
    }
  }

  /** Parser. */
  private class Parser {
    final List<Command> commands = new ArrayList<Command>();

    Command parse() {
      for (;;) {
        Command command;
        try {
          command = nextCommand();
        } catch (IOException e) {
          throw new RuntimeException("Error while reading next command", e);
        }
        if (command == null) {
          break;
        }
        commands.add(command);
      }
      return of(commands);
    }

    private Command nextCommand() throws IOException {
      lines.clear();
      ImmutableList<String> content = ImmutableList.of();
      for (;;) {
        String line = nextLine();
        if (line == null) {
          return null;
        }
        if (line.startsWith("#") || line.isEmpty()) {
          return new CommentCommand(lines);
        }
        if (line.startsWith("!")) {
          line = line.substring(1);
          while (line.startsWith(" ")) {
            line = line.substring(1);
          }
          if (line.startsWith("use")) {
            String[] parts = line.split(" ");
            return new UseCommand(lines, parts[1]);
          }
          if (line.startsWith("ok")) {
            SqlCommand command = previousSqlCommand();
            return new CheckResultCommand(lines, command, content);
          }
          if (line.startsWith("plan")) {
            SqlCommand command = previousSqlCommand();
            return new ExplainCommand(lines, command, content);
          }
          if (line.startsWith("error")) {
            SqlCommand command = previousSqlCommand();
            return new ErrorCommand(lines, command, content);
          }
          if (line.startsWith("skip")) {
            return new SkipCommand(lines);
          }
          if (line.startsWith("set outputformat")) {
            String[] parts = line.split(" ");
            final OutputFormat outputFormat =
                OutputFormat.valueOf(parts[2].toUpperCase());
            return new SetCommand(lines, Property.OUTPUTFORMAT, outputFormat);
          }
          if (line.equals("if (false) {")) {
            List<String> ifLines = ImmutableList.copyOf(lines);
            lines.clear();
            Command command = new Parser().parse();
            return new IfCommand(ifLines, lines, command, false);
          }
          if (line.equals("if (true) {")) {
            List<String> ifLines = ImmutableList.copyOf(lines);
            lines.clear();
            Command command = new Parser().parse();
            return new IfCommand(ifLines, lines, command, true);
          }
          if (line.equals("}")) {
            return null;
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
          buf.append(line).append("\n");
          if (last) {
            break;
          }
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
          return new SqlCommand(content, sql, null);
        }
      }
    }

    private SqlCommand previousSqlCommand() {
      for (int i = commands.size() - 1; i >= 0; i--) {
        Command command = commands.get(i);
        if (command instanceof SqlCommand) {
          return (SqlCommand) command;
        }
      }
      throw new AssertionError("no previous SQL command");
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

  /** Schemes for converting the output of a SQL statement into text. */
  enum OutputFormat {
    CSV {
      @Override public void format(ResultSet resultSet,
          List<String> headerLines, List<String> bodyLines,
          List<String> footerLines, Quidem run) throws Exception {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int n = metaData.getColumnCount();
        final StringBuilder buf = new StringBuilder();
        for (int i = 0; i < n; i++) {
          if (i > 0) {
            buf.append(", ");
          }
          buf.append(metaData.getColumnLabel(i + 1));
        }
        headerLines.add(buf.toString());
        buf.setLength(0);
        final List<String> lines = Lists.newArrayList();
        while (resultSet.next()) {
          for (int i = 0; i < n; i++) {
            if (i > 0) {
              buf.append(", ");
            }
            buf.append(resultSet.getString(i + 1));
          }
          lines.add(buf.toString());
          buf.setLength(0);
        }
        if (run.sort) {
          Collections.sort(lines);
        }
        bodyLines.addAll(lines);
      }
    },

    // Example:
    //
    //  ename | deptno | gender | first_value
    // -------+--------+--------+-------------
    //  Jane  |     10 | F      | Jane
    //  Bob   |     10 | M      | Jane
    // (2 rows)
    PSQL {
      @Override public void format(ResultSet resultSet,
          List<String> headerLines, List<String> bodyLines,
          List<String> footerLines, Quidem run) throws Exception {
        Quidem.format(
            resultSet, headerLines, bodyLines, footerLines, run.sort, false);
      }
    },

    // Example:
    //
    // +-------+--------+--------+-------------+
    // | ename | deptno | gender | first_value |
    // +-------+--------+--------+-------------+
    // | Jane  |     10 | F      | Jane        |
    // | Bob   |     10 | M      | Jane        |
    // +-------+--------+--------+-------------+
    // (2 rows)
    MYSQL {
      @Override public void format(ResultSet resultSet,
          List<String> headerLines, List<String> bodyLines,
          List<String> footerLines, Quidem run) throws Exception {
        Quidem.format(
            resultSet, headerLines, bodyLines, footerLines, run.sort, true);
      }
    };

    public abstract void format(ResultSet resultSet, List<String> headerLines,
        List<String> bodyLines, List<String> footerLines, Quidem run)
        throws Exception;
  }

  private static void format(ResultSet resultSet, List<String> headerLines,
      List<String> bodyLines, List<String> footerLines, boolean sort,
      boolean mysql) throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int n = metaData.getColumnCount();
    final int[] widths = new int[n];
    final List<String[]> rows = new ArrayList<String[]>();
    final boolean[] rights = new boolean[n];

    for (int i = 0; i < n; i++) {
      widths[i] = metaData.getColumnLabel(i + 1).length();
    }
    while (resultSet.next()) {
      String[] row = new String[n];
      for (int i = 0; i < n; i++) {
        String value = resultSet.getString(i + 1);
        widths[i] = Math.max(widths[i], value == null ? 0 : value.length());
        row[i] = value;
      }
      rows.add(row);
    }
    for (int i = 0; i < widths.length; i++) {
      switch (metaData.getColumnType(i + 1)) {
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.FLOAT:
      case Types.REAL:
      case Types.DOUBLE:
      case Types.NUMERIC:
      case Types.DECIMAL:
        rights[i] = true;
      }
    }

    if (sort) {
      Collections.sort(rows, ORDERING);
    }

    // Compute "+-----+---+" (if b)
    // or       "-----+---" (if not b)
    final StringBuilder buf = new StringBuilder();
    for (int i = 0; i < n; i++) {
      buf.append(mysql || i > 0 ? "+" : "");
      buf.append(chars('-', widths[i] + 2));
    }
    buf.append(mysql ? "+" : "");
    String hyphens = flush(buf);

    if (mysql) {
      headerLines.add(hyphens);
    }

    // Print "| FOO | B |"
    // or    "  FOO | B"
    for (int i = 0; i < n; i++) {
      buf.append(i > 0 ? " | " : mysql ? "| " : " ");
      final String label = metaData.getColumnLabel(i + 1);
      buf.append(mysql || i < n - 1 ? pad(label, widths[i], false) : label);
    }
    buf.append(mysql ? " |" : "");
    headerLines.add(flush(buf));
    headerLines.add(hyphens);
    for (String[] row : rows) {
      for (int i = 0; i < n; i++) {
        buf.append(i > 0 ? " | " : mysql ? "| " : " ");
        // don't pad the last field if it is left-justified
        final String s = !mysql && i == n - 1 && !rights[i]
            ? row[i]
            : pad(row[i], widths[i], rights[i]);
        buf.append(s);
      }
      buf.append(mysql ? " |" : "");
      bodyLines.add(flush(buf));
    }
    if (mysql) {
      footerLines.add(hyphens);
    }
    footerLines.add(
        rows.size() == 1 ? "(1 row)" : "(" + rows.size() + " rows)");
    footerLines.add("");
  }

  /** Returns the contents of a StringBuilder and clears it for the next use. */
  private static String flush(StringBuilder buf) {
    final String s = buf.toString();
    buf.setLength(0);
    return s;
  }

  /** Command. */
  interface Command {
    void execute(boolean execute) throws Exception;
  }

  /** Base class for implementations of Command. */
  abstract class AbstractCommand implements Command {
    protected Command echo(Iterable<String> lines) {
      for (String line : lines) {
        try {
          printWriter.println(line);
        } catch (Exception e) {
          throw new RuntimeException("Error while writing output", e);
        }
      }
      return this;
    }
  }

  /** Base class for implementations of Command that have one piece of source
   * code. */
  abstract class SimpleCommand extends AbstractCommand {
    protected final ImmutableList<String> lines;

    public SimpleCommand(List<String> lines) {
      this.lines = ImmutableList.copyOf(lines);
    }
  }

  /** Command that sets the current connection. */
  class UseCommand extends SimpleCommand {
    private final String name;

    public UseCommand(List<String> lines, String name) {
      super(lines);
      this.name = name;
    }

    public void execute(boolean execute) throws Exception {
      echo(lines);
      if (connection != null) {
        connection.close();
      }
      connection = connectionFactory.connect(name);
    }
  }

  /** Command that executes a SQL statement and checks its result. */
  class CheckResultCommand extends SimpleCommand {
    private final SqlCommand sqlCommand;
    protected final ImmutableList<String> output;

    public CheckResultCommand(List<String> lines, SqlCommand sqlCommand,
        ImmutableList<String> output) {
      super(lines);
      this.sqlCommand = sqlCommand;
      this.output = output;
    }

    @Override public String toString() {
      return "CheckResultCommand [sql: " + sqlCommand.sql + "]";
    }

    public void execute(boolean execute) throws Exception {
      if (execute) {
        if (connection == null) {
          throw new RuntimeException("no connection");
        }
        final Statement statement = connection.createStatement();
        if (resultSet != null) {
          resultSet.close();
        }
        try {
          if (DEBUG) {
            System.out.println("execute: " + this);
          }
          resultSet = null;
          resultSetException = null;
          resultSet = statement.executeQuery(sqlCommand.sql);
          sort = !sqlCommand.sql.toUpperCase().contains("ORDER BY");
        } catch (SQLException e) {
          resultSetException = e;
        }
        if (resultSet != null) {
          final OutputFormat format =
              (OutputFormat) map.get(Property.OUTPUTFORMAT);
          final List<String> headerLines = new ArrayList<String>();
          final List<String> bodyLines = new ArrayList<String>();
          final List<String> footerLines = new ArrayList<String>();
          format.format(resultSet, headerLines, bodyLines, footerLines,
              Quidem.this);

          // Construct the original body.
          // Strip the header and footer from the actual output.
          // We assume that original and actual header have the same line count.
          // Ditto footers.
          final List<String> lines = new ArrayList<String>(output);
          for (String line : headerLines) {
            if (!lines.isEmpty()) {
              lines.remove(0);
            }
          }
          for (String line : footerLines) {
            if (!lines.isEmpty()) {
              lines.remove(lines.size() - 1);
            }
          }

          // Print the actual header.
          for (String line : headerLines) {
            printWriter.println(line);
          }
          // Print all lines that occurred in the actual output ("bodyLines"),
          // but in their original order ("lines").
          for (String line : lines) {
            if (sort) {
              if (bodyLines.remove(line)) {
                printWriter.println(line);
              }
            } else {
              if (!bodyLines.isEmpty()
                  && bodyLines.get(0).equals(line)) {
                bodyLines.remove(0);
                printWriter.println(line);
              }
            }
          }
          // Print lines that occurred in the actual output but not original.
          for (String line : bodyLines) {
            printWriter.println(line);
          }
          // Print the actual footer.
          for (String line : footerLines) {
            printWriter.println(line);
          }
          resultSet.close();
        }

        checkResultSet(resultSetException);

        if (resultSet == null && resultSetException == null) {
          throw new AssertionError("neither resultSet nor exception set");
        }
        resultSet = null;
        resultSetException = null;
      } else {
        echo(output);
      }
      echo(lines);
    }

    protected void checkResultSet(SQLException resultSetException) {
      if (resultSetException != null) {
        resultSetException.printStackTrace(printWriter);
      }
    }
  }

  /** Command that executes a SQL statement and checks that it throws a given
   * error. */
  class ErrorCommand extends CheckResultCommand {
    public ErrorCommand(List<String> lines, SqlCommand sqlCommand,
        ImmutableList<String> output) {
      super(lines, sqlCommand, output);
    }

    @Override protected void checkResultSet(SQLException resultSetException) {
      if (resultSetException == null) {
        printWriter.println("Expected error, but SQL command did not give one");
      } else if (!output.isEmpty()
          && stack(resultSetException).contains(concat(output))) {
        // They gave an expected error, and the actual error does not match.
        // Print the actual error. This will cause a diff.
        for (String line : output) {
          printWriter.println(line);
        }
      } else {
        super.checkResultSet(resultSetException);
      }
    }

    private String stack(Throwable e) {
      final StringWriter buf = new StringWriter();
      e.printStackTrace(new PrintWriter(buf));
      return buf.toString();
    }

    private String concat(List<String> lines) {
      final StringBuilder buf = new StringBuilder();
      for (String line : lines) {
        buf.append(line.trim());
        buf.append("\n");
      }
      return buf.toString();
    }
  }

  /** Command that prints the plan for the current query. */
  class ExplainCommand extends SimpleCommand {
    private final SqlCommand sqlCommand;
    private final ImmutableList<String> content;

    public ExplainCommand(List<String> lines,
        SqlCommand sqlCommand,
        ImmutableList<String> content) {
      super(lines);
      this.sqlCommand = sqlCommand;
      this.content = content;
    }

    @Override public String toString() {
      return "ExplainCommand [sql: " + sqlCommand.sql + "]";
    }

    public void execute(boolean execute) throws Exception {
      if (execute) {
        final Statement statement = connection.createStatement();
        final ResultSet resultSet =
            statement.executeQuery("explain plan for " + sqlCommand.sql);
        final StringBuffer buf = new StringBuffer();
        while (resultSet.next()) {
          final String line = resultSet.getString(1);
          buf.append(line);
          if (!line.endsWith("\n")) {
            buf.append("\n");
          }
        }
        if (buf.length() == 0) {
          throw new AssertionError("explain returned 0 records");
        }
        printWriter.print(buf);
        printWriter.flush();
      } else {
        echo(content);
      }
      echo(lines);
    }
  }

  /** Command that executes a SQL statement. */
  private class SqlCommand extends SimpleCommand {
    private final String sql;

    protected SqlCommand(List<String> lines, String sql, List<String> output) {
      super(lines);
      this.sql = Preconditions.checkNotNull(sql);
    }

    public void execute(boolean execute) throws Exception {
      echo(lines);
    }
  }

  /** Creates a connection for a given name.
   * Kind of a directory service.
   * Caller must close the connection. */
  public interface ConnectionFactory {
    Connection connect(String name) throws Exception;
  }

  /** Property whose value may be set. */
  enum Property {
    OUTPUTFORMAT
  }

  /** Command that defines the current SQL statement.
   *
   * @see CheckResultCommand
   * @see ExplainCommand
   */
  class SetCommand extends SimpleCommand {
    private final Property property;
    private final Object value;

    public SetCommand(List<String> lines, Property property, Object value) {
      super(lines);
      this.property = property;
      this.value = value;
    }

    public void execute(boolean execute) throws Exception {
      echo(lines);
      map.put(property, value);
    }
  }

  /** Command that executes a comment. (Does nothing.) */
  class CommentCommand extends SimpleCommand {
    public CommentCommand(List<String> lines) {
      super(lines);
    }

    public void execute(boolean execute) throws Exception {
      echo(lines);
    }
  }

  /** Command that disables execution of a block. */
  class IfCommand extends AbstractCommand {
    private final List<String> ifLines;
    private final List<String> endLines;
    private final Command command;
    private final boolean enable;

    public IfCommand(List<String> ifLines, List<String> endLines,
        Command command, boolean enable) {
      this.enable = enable;
      this.ifLines = ImmutableList.copyOf(ifLines);
      this.endLines = ImmutableList.copyOf(endLines);
      this.command = command;
    }

    public void execute(boolean execute) throws Exception {
      echo(ifLines);
      // Switch to a mode where we don't execute, just echo.
      boolean oldExecute = Quidem.this.execute;
      boolean newExecute;
      if (skip) {
        // If "skip" is set, stay in current (disabled) mode.
        newExecute = oldExecute;
      } else {
        // If "enable" is true, stay in the current mode.
        newExecute = enable;
      }
      command.execute(newExecute);
      echo(endLines);
    }
  }

  /** Command that switches to a mode where we skip executing the rest of the
   * input. The input is still printed. */
  class SkipCommand extends SimpleCommand {
    public SkipCommand(List<String> lines) {
      super(lines);
    }

    public void execute(boolean execute) throws Exception {
      echo(lines);
      // Switch to a mode where we don't execute, just echo.
      // Set "skip" so we don't leave that mode.
      skip = true;
      Quidem.this.execute = false;
    }
  }

  /** Command that executes a comment. (Does nothing.) */
  class CompositeCommand extends AbstractCommand {
    private final List<Command> commands;

    public CompositeCommand(List<Command> commands) {
      this.commands = commands;
    }

    public void execute(boolean execute) throws Exception {
      // We handle all RuntimeExceptions, all Exceptions, and a limited number
      // of Errors. If we don't understand an Error (e.g. OutOfMemoryError)
      // then we print it out, then abort.
      for (Command command : commands) {
        boolean abort = false;
        Throwable e = null;
        try {
          command.execute(execute && Quidem.this.execute);
        } catch (RuntimeException e0) {
          e = e0;
        } catch (Exception e0) {
          e = e0;
        } catch (AssertionError e0) {
          // We handle a limited number of errors.
          e = e0;
        } catch (Throwable e0) {
          e = e0;
          abort = true;
        }
        if (e != null) {
          command.execute(false); // echo the command
          printWriter.println("Error while executing command " + command);
          e.printStackTrace(printWriter);
          if (abort) {
            throw (Error) e;
          }
        }
      }
    }
  }
}

// End Quidem.java
