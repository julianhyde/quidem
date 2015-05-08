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

For more examples, look at the `.oq` files in the
<a href="https://github.com/apache/incubator-calcite/tree/master/core/src/test/resources/sql">Apache Calcite</a>
project.

## Run

```bash
$ java net.hydromatic.quidem.Quidem --db hr jdbc:mysql://localhost/hr scott tiger script.oq
```

Synopsis

```
quidem argument... filename
```

Arguments
* `--help` - Display help
* `--db dbName jdbcUrl username password` - Define database, so that
  you can execute `!use dbName`.
* `--factory className` - Define a connection factory. The class must
  implement interface
  `net.hydromatic.quidem.Quidem.ConnectionFactory`.

## Script commands

### `# a comment line`

Comments are printed and not executed.
The line must start with a `#`.

### `<sql statement>;`

Sets the current query to `<sql statement>` (`SELECT`, `INSERT`, etc.)
Queries may span multiple lines, and must be terminated with a semi-colon, ';'.

The same query may be used by several `!ok` and `!plan` commands.

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

Condition must be `true` or `false`.
* If `false`, Quidem does not execute commands, but prints their current output.
* If `true`, Quidem executes commands as normal.

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
```

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

### `!set outputformat <format>`

Sets the output format (see `!ok`).

Options are `csv`, `mysql`, `psql`:

```sql
select * from emp;

!set outputformat csv
ename,deptno,gender,first_value
Jane,10,F,Jane
Bob,10,M,Jane
!ok

!set outputformat mysql
+-------+--------+--------+-------------+
| ename | deptno | gender | first_value |
+-------+--------+--------+-------------+
| Jane  |     10 | F      | Jane        |
| Bob   |     10 | M      | Jane        |
+-------+--------+--------+-------------+
(2 rows)
!ok

!set outputformat psql
 ename | deptno | gender | first_value
-------+--------+--------+-------------
 Jane  |     10 | F      | Jane
 Bob   |     10 | M      | Jane
(2 rows)
!ok
```

### `!skip`

Switches to a mode where we skip executing the rest of the
input. The input is still printed.

The effect is similar to enclosing the remainder of the script in an
`!if (false) {` ... `!}` block.

### `!use <db>`

Uses a connection to the `db` database from now until the end of
the script or the next `!use` command.

`db` must be a database name defined using a connection factory
(or the `--db` command-line argument).
Connection details such as URLs, username, password and preferred
driver are presumably provided within the connection factory.
Including them within the script would not be portable or maintainable.

## Get Quidem

### From Maven

Get Quidem from the
<a href="http://conjars.org/net.hydromatic/quidem">conjars.org</a>
maven repository:

```xml
<dependency>
  <groupId>net.hydromatic</groupId>
  <artifactId>quidem</artifactId>
  <version>0.1.1</version>
</dependency>
```

### Download and build

You need Java (1.6 or higher; 1.8 preferred), git and maven (3.2.1 or higher).

```bash
$ git clone git://github.com/julianhyde/quidem.git
$ cd quidem
$ mvn compile
```

## More information

* License: Apache License, Version 2.0
* Author: Julian Hyde
* Blog: http://julianhyde.blogspot.com
* Project page: http://www.hydromatic.net/quidem
* Source code: http://github.com/julianhyde/quidem
* Developers list:
  <a href="mailto:dev@calcite.incubator.apache.org">dev at calcite.incubator.apache.org</a>
  (<a href="http://mail-archives.apache.org/mod_mbox/incubator-calcite-dev/">archive</a>,
  <a href="mailto:dev-subscribe@calcite.incubator.apache.org">subscribe</a>)
* Issues: https://github.com/julianhyde/quidem/issues
* <a href="HISTORY.md">Release notes and history</a>
