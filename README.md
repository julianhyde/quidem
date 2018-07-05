[![Build Status](https://travis-ci.org/julianhyde/quidem.svg?branch=master)](https://travis-ci.org/julianhyde/quidem)

# Quidem

Quidem is an idempotent query executor.

It is a scripting language for testing databases.

## Why Quidem?

If you are testing a database system, you would traditionally take one
of two approaches.
* __Scripting__. Write a SQL script (interpreted by an
  engine such as <a href="http://github.com/julianhyde/sqlline">sqlline</a>),
  and a reference or 'golden' file containing the expected output.
  Run the SQL script, and compare the output with the golden file.
* __Procedural__. Write a test program in a language such as Java,
  which executes queries and verifies the results using assertions.

Both of these approaches have problems. With scripting, you have
two sets of files to keep in sync as you merge patches.
Scripting languages do not have much procedural
power, so you cannot conditionally execute a test. If you want to both
execute a query and show its plan, you have to write the SQL
twice. And, the only way you can verify a query is using textual
comparison, so your tests will break if there are slight
changes in output, such as rows being returned in a different order.

Procedural tests are more powerful, but more difficult to write. You
have to include query strings in Java string literals, taking care to
escape any quotes in the query string. If you choose to validate the
output of a query and that output changes, you need to laboriously
copy-paste the correct output into a string literal. Lastly, you limit
who could contribute to your project, because not all potential
contributors are comfortable writing procedural code.

Quidem takes a middle path. Writing a test is as convenient and
concise as writing a script, but you can include procedural constructs
such as assertions and conditional execution.

*Quidem* means _<b>qu</b>ery <b>idem</b>potentently_.
If a script succeeds, its output is identical to its input.
This means that there are no reference files;
the expected output is interleaved with the input.
If the output has changed, you can accept the new
output by simply overwriting the script file with the new output.

*Quidem* is also the Latin word for
<a href="https://translate.google.com/#la/en/quidem">*indeed*, *in fact* or *in truth*</a>.

## An example

Let's suppose you wish to connect to the `hr` database, run a
query, and checks its output and plan. Write the following script,
and save it as `src/test/resources/iq/emp.iq`:

```sql
!use hr
select * from emp where deptno <= 30;
!ok
!plan
```

Run it using maven's surefire plugin, an quidem creates
the following output in `target/surefire/iq/emp.iq`:

```sql
!use hr
select * from emp where deptno <= 30;
+-------+--------+--------+
| ENAME | DEPTNO | GENDER |
+-------+--------+--------+
| Jane  |     10 | F      |
| Bob   |     10 | M      |
| Eric  |     20 | M      |
| Susan |     30 | F      |
+-------+--------+--------+
(4 rows)

!ok
EnumerableFilterRel(condition=[<=($1, 30)]
  EnumerableTableAccessRel(table=[[hr, emp]])
!plan
```

Output is mixed in with input.
The output of each command actually occurs just *before* the command:
* The `!ok` command executes a query and compares its output. Quidem
  notices that the query has no `ORDER BY` clause, so would accept the 4
  rows in any order.
* The `!plan` command runs `EXPLAIN` for the most recently declared SQL
  statement.

This output looks good, so we do a strange thing:

```bash
$ cp target/surefire/iq/emp.iq src/test/resources/iq/emp.iq
```

This command overwrites the script with its output.
But remember, quidem is idempotent: the output of any script is
executable, and the output of a correct script is itself.

Run the script again, and sure enough, it passes.

We can easily make incremental changes to a script, such as adding comments,
re-ordering statements, or conditionally disabling parts of the script,
without introducing any differences. Other changes, such as adding
SQL statements, tend to require only local changes to that part of the file,
and therefore Quidem scripts are friendly to patches, merges and parallel development.

For more examples, look at the `.iq` files in the
<a href="https://github.com/apache/calcite/tree/master/core/src/test/resources/sql">Apache Calcite</a>
project.

## Run

```bash
$ java net.hydromatic.quidem.Quidem --db hr jdbc:mysql://localhost/hr scott tiger script.iq
```

Synopsis

```
quidem argument... filename
```

Arguments
* `--help` - Display help
* `--var name value` - Define a variable that can be used in `!if (name)`.
* `--db dbName jdbcUrl username password` - Define database, so that
  you can execute `!use dbName`.
* `--factory className` - Define a connection factory. The class must
  implement interface
  `net.hydromatic.quidem.Quidem.ConnectionFactory`.
* `--command-handler className` - Define a command handler. The class
  must implement interface `net.hydromatic.quidem.CommandHandler`.

For example, the following command line runs Quidem script `script.iq`
with database `hr` defined to connect to MySQL,
and variable `advanced` set to `true`.

```
quidem --db hr jdbc:mysql://localhost/hr scott tiger --var advanced true script.iq
```

## Script commands

### `# a comment line`

Comments are printed and not executed.
The line must start with a `#`.

### `<sql statement>;`

Sets the current query to `<sql statement>` (`SELECT`, `INSERT`, etc.)
Queries may span multiple lines, and must be terminated with a semi-colon, ';'.

The same query may be used by several `!ok`, `!verify`, `!update`, `!error`,
`!type` and `!plan` commands.

### `!error`

Executes the current query and checks that it returns a particular error.

The command succeeds if the expected error is non-empty and occurs somewhere
within the actual error stack. Spaces and line endings do not need to match
exactly.

Example:

```bash
# Match just the error message
select blah from blah;
user lacks privilege or object not found: BLAH
!error

# Match the error class, message and the first few lines of the stack
select blah from blah;
java.sql.SQLSyntaxErrorException: user lacks privilege or object not found: BLAH
  at org.hsqldb.jdbc.JDBCUtil.sqlException(Unknown Source)
  at org.hsqldb.jdbc.JDBCUtil.sqlException(Unknown Source)
!error
```

The `!error` command fails, and prints the full error stack, if:
* the SQL statement does not give an error, or
* the expected error is empty, or
* actual error does not match the expected error

You can then edit the stack, or cut it down to just the parts of
the message that you care about.

### `!if (condition)`

Condition must be `true`, `false`, or a variable name.
* If `false`, Quidem does not execute commands, but prints their current output.
* If `true`, Quidem executes commands as normal.
* If a variable (obtained from the `env` parameter passed to Quidem's
  constructor, from `--var` on the command line),
  and the variable is boolean true or the string "true",
  behavior is as `true` above, otherwise as `false` above.
  Unset variables are treated as `false`.

Commands must be enclosed in `{` and terminate with a `!}` line.

Example:

```sql
!if (false) {
select c from t limit 10;
+--------------------------------------+
| C                                    |
+--------------------------------------+
| Any old text here. It not be ignored |
| until the query is enabled.          |
+--------------------------------------+
!ok
!}

!if (jdk18) {
values 'We must be running JDK 1.8.';
+-----------------------------+
| C                           |
+-----------------------------+
| We must be running JDK 1.8. |
+-----------------------------+
(1 rows)
!ok
!}
```

Variable names can consist of multiple parts. For example, you can write

```sql
!if (calcite.version.isAtLeast1) {
values 'Hello';
!ok
!}
```

and `calcite.version.isAtLeast1` will evaluate to true if
`env` has an entry "calcite" of type `Function<String, Object>`,
that has an entry "version" of type `Function<String, Object>`,
that has an entry "isAtLeast1" that is either true
or an object whose `toString()` method returns "true".

A multi-part variable evaluates to false if any of its parts are null,
or any part before the last is not a `Function<String, Object>`.

### `!ok`

Executes the current query and prints the formatted output.
The output appears before the `!ok` line.

If the query is unordered, accepts the output in any order.
(Since the output will be compared using `diff`, the Quidem engine
parses the output in the script and attempts to display the actual
query output in the same order.)

Example:

```bash
select * from emp where deptno <= 30;
+-------+--------+--------+
| ENAME | DEPTNO | GENDER |
+-------+--------+--------+
| Jane  |     10 | F      |
| Bob   |     10 | M      |
| Eric  |     20 | M      |
| Susan |     30 | F      |
+-------+--------+--------+
(4 rows)

!ok
```

### `!plan`

Shows the plan of the current query.
The output appears before the `!plan` line.

Example:

```bash
select * from emp where deptno <= 30;
EnumerableFilterRel(condition=[<=($1, 30)]
  EnumerableTableAccessRel(table=[[hr, emp]])
!plan
```

### `!push <variable> <value>`

Sets the value of a variable, saving the previous value so that
it can be restored using `!pop`.

### `!pop <variable>`

Restores the value of a variable to its value before the
previous `!push` of that variable.
It is an error if there was no previous `!push`.

### `!set <variable> <value>`

Sets the value of a variable.

The variable name must start with a letter, be followed by letters or
digits, and is case-sensitive.

The value is a boolean (`true` or `false`),
an integer,
or a string (either one word, or a double-quoted string).

### `!set outputformat <format>`

Sets the output format (see `!ok`).

Options are `csv`, `mysql`, `oracle`, `psql`:

```sql
select * from emp;

!set outputformat csv
ename,deptno,gender
Jane,10,F
Bob,10,M
!ok

!set outputformat mysql
+-------+--------+--------+
| ename | deptno | gender |
+-------+--------+--------+
| Jane  |     10 | F      |
| Bob   |     10 | M      |
+-------+--------+--------+
(2 rows)

!ok

!set outputformat oracle
ename deptno gender
===== ====== ======
Jane      10 F
Bob       10 M

2 rows selected.

!ok

!set outputformat psql
 ename | deptno | gender
-------+--------+--------
 Jane  |     10 | F
 Bob   |     10 | M
(2 rows)

!ok
```

### `!skip`

Switches to a mode where we skip executing the rest of the
input. The input is still printed.

The effect is similar to enclosing the remainder of the script in an
`!if (false) {` ... `!}` block.

### `!type`

Prints the column types of the current SQL statement.

Example:

```sql
select  empno, deptno, sal from scott.emp;
EMPNO SMALLINT(16) NOT NULL
DEPTNO TINYINT(8)
SAL DECIMAL(7, 2)
!type
```

### `!update`

Executes a DML command (INSERT, UPDATE or DELETE) and prints the
number of rows processed.

Example:

```sql
update emp
set sal = sal * 2
where deptno = 10;
(2 rows modified)

!update
```

### `!use <db>`

Uses a connection to the `db` database from now until the end of
the script or the next `!use` command.

`db` must be a database name defined using a connection factory
(or the `--db` command-line argument).
Connection details such as URLs, username, password and preferred
driver are presumably provided within the connection factory.
Including them within the script would not be portable or maintainable.

### `!verify`

Verifies the output of the current statement by executing the same
statement on the reference database.

Fails if the current connection has no reference database,
or if the statement is invalid on the reference database,
or if the output is different.

Example:

```sql
select count(*) from scott.emp;
!verify
```

The `!verify` command is a nice alternative to the `!ok` command
because it reduces the chance of human error checking in the wrong result.

### `!<custom command>`

Runs a custom command recognized by a custom command handler.
See the `--command-handler` command-line argument.

## Get Quidem

### From Maven

Get Quidem from
<a href="https://search.maven.org/#search%7Cga%7C1%7Ca%3Aquidem">Maven central</a>:

```xml
<dependency>
  <groupId>net.hydromatic</groupId>
  <artifactId>quidem</artifactId>
  <version>0.8</version>
</dependency>
```

### Download and build

You need Java (8 or higher; 9 preferred), git and maven (3.2.1 or higher).

```bash
$ git clone git://github.com/julianhyde/quidem.git
$ cd quidem
$ mvn compile
```

## More information

* License: <a href="LICENSE">Apache Software License, Version 2.0</a>
* Author: Julian Hyde
* Blog: http://julianhyde.blogspot.com
* Project page: http://www.hydromatic.net/quidem
* API: http://www.hydromatic.net/quidem/apidocs
* Source code: http://github.com/julianhyde/quidem
* Developers list:
  <a href="mailto:dev@calcite.apache.org">dev at calcite.apache.org</a>
  (<a href="http://mail-archives.apache.org/mod_mbox/calcite-dev/">archive</a>,
  <a href="mailto:dev-subscribe@calcite.apache.org">subscribe</a>)
* Issues: https://github.com/julianhyde/quidem/issues
* <a href="HISTORY.md">Release notes and history</a>
* <a href="HOWTO.md">HOWTO</a>
