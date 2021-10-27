# Quidem release history

For a full list of releases, see <a href="https://github.com/julianhyde/quidem/releases">github</a>.

## <a href="https://github.com/julianhyde/quidem/releases/tag/quidem-0.10">0.10</a> / 2021-11-06

* Upgrade `maven-javadoc-plugin`
* [<a href="https://github.com/julianhyde/quidem/issues/23">QUIDEM-23</a>]
  Catch exceptions that happen during `ResultSet.next()`
* Run tests in parallel
* Upgrade Junit (4.11 &rarr; 5.8.1) and Hamcrest (1.3 &rarr; 2.2)
* Reorganize tests
* Allow Java 17 and Guava 31.0.1-jre
* Remove uses of deprecated `org.junit.Assert` methods
* Travis: quote variables, skip install
* Maven wrapper
* Upgrade plugins and dependencies
* Remove Conjars repository
* Docker login
* Rename 'master' branch to 'main'
* In Travis CI, use docker
* Add a maven property for the version of each dependency and plugin

## <a href="https://github.com/julianhyde/quidem/releases/tag/quidem-0.9">0.9</a> / 2018-07-06

* [<a href="https://github.com/julianhyde/quidem/issues/20">QUIDEM-20</a>]
  Require Java 8 or higher
* [<a href="https://github.com/julianhyde/quidem/issues/18">QUIDEM-18</a>]
  Custom command handler
* Make `Command` static, abstracting `Quidem` engine via `Command.Context`
  facade
* [<a href="https://github.com/julianhyde/quidem/issues/19">QUIDEM-19</a>]
  Set Quidem parameters via `Config` and `ConfigBuilder`
* Test using `!ok` to run DML command
* Require Java 1.7 or higher
* Use `maven-enforcer-plugin` to check Java and Maven version
* [<a href="https://github.com/julianhyde/quidem/issues/17">QUIDEM-17</a>]
  Trim trailing spaces
* Silence warnings about trusty being deprecated; we need it for JDK9
* [<a href="https://github.com/julianhyde/quidem/issues/16">QUIDEM-16</a>]
  Add `oracle` format
* [<a href="https://github.com/julianhyde/quidem/issues/15">QUIDEM-15</a>]
  JDK9 support

## <a href="https://github.com/julianhyde/quidem/releases/tag/quidem-0.8">0.8</a> / 2016-10-27

* Catch exceptions that are not `SQLException` (drivers are not supposed
  to throw them, but sometimes do)
* [<a href="https://github.com/julianhyde/quidem/issues/14">QUIDEM-14</a>]
  User-defined properties, and `!set`, `!push`, `!pop` and `!show`
  commands
* Remove deprecated APIs; deprecate `NewConnectionFactory`

## <a href="https://github.com/julianhyde/quidem/releases/tag/quidem-0.7">0.7</a> / 2015-12-03

* [<a href="https://github.com/julianhyde/quidem/issues/12">QUIDEM-12</a>]
  Add `!verify` command that compares output to a reference database
* [<a href="https://github.com/julianhyde/quidem/issues/11">QUIDEM-11</a>]
  Nested variables
* Add deprecated constructor for backwards compatibility
* [<a href="https://github.com/julianhyde/quidem/issues/9">QUIDEM-9</a>]
  Add `!type` command that checks query column types
* Work around
  [<a href="https://github.com/ktoso/maven-git-commit-id-plugin/issues/63">maven-git-commit-it-plugin#63</a>]
* [<a href="https://github.com/julianhyde/quidem/issues/10">QUIDEM-10</a>]
  Add `!update` command that calls `executeUpdate` and checks update count
  (Mike Hinchey)
* Close statement after `!ok` and `!plan` commands (Mike Hinchey)
* Factor command-line parsing into a `Launcher` class
* Add <a href="HOWTO.md">HOWTO</a>
* Fix release number and maven location

## <a href="https://github.com/julianhyde/quidem/releases/tag/quidem-0.6">0.6</a> / 2015-07-19

* Add `--var` argument, so you can pass variables from the command line
* [<a href="https://github.com/julianhyde/quidem/issues/7">QUIDEM-7</a>]
  Don't be fooled by ORDER BY inside windowed aggregate
* [<a href="https://github.com/julianhyde/quidem/issues/8">QUIDEM-8</a>]
  Allow variables in `if`
* Truncate error stack if it exceeds N characters
* Deploy to maven central

## <a href="https://github.com/julianhyde/quidem/releases/tag/quidem-0.5">0.5</a> / 2015-05-15

* [<a href="https://github.com/julianhyde/quidem/issues/6">QUIDEM-6</a>]
  Be permissive when matching whitespace in error stacks
* [<a href="https://github.com/julianhyde/quidem/issues/5">QUIDEM-5</a>]
  Windows line endings

## <a href="https://github.com/julianhyde/quidem/releases/tag/quidem-0.4">0.4</a> / 2015-05-08

* [<a href="https://github.com/julianhyde/quidem/issues/1">QUIDEM-1</a>]
  Add '!error' command
* [<a href="https://github.com/julianhyde/quidem/issues/3">QUIDEM-3</a>]
  Trailing spaces in psql output format
* [<a href="https://github.com/julianhyde/quidem/issues/4">QUIDEM-4</a>]
  Use "scott" rather than than "foodmart" as test data set
* Use net.hydromatic parent POM
* Fix `!skip` command
* Document '!error' and '!skip' commands
* In Travis CI, generate site
* Fluent testing API
* In Travis CI, enable containers, and cache .m2 directory

## <a href="https://github.com/julianhyde/quidem/releases/tag/quidem-0.3">0.3</a> / 2015-03-04

* Set distribution repository to Maven central rather than Conjars
* Sign jars
* Fix license URL
* Rename a few last Optiq to Calcite

## <a href="https://github.com/julianhyde/quidem/releases/tag/quidem-0.2">0.2</a> / 2014-12-09

* [<a href="https://github.com/julianhyde/quidem/issues/2">QUIDEM-2</a>]
  Right-justify numeric columns
* Re-order actual rows to match expected rows, but only if query does not
  contain `ORDER BY`
* Rename 'Optiq' to 'Calcite'
* Add maven coordinates to README
* Add `quidem` bash script and a simple example script
* Add command-line arguments `--help`, `--db`, `--factory`

## <a href="https://github.com/julianhyde/quidem/releases/tag/quidem-0.1.1">0.1.1</a> / 2014-09-21

* Ensure that each line from `EXPLAIN` has exactly one line-ending

## <a href="https://github.com/julianhyde/quidem/releases/tag/quidem-0.1">0.1</a> / 2014-09-21

* Convert test suite to hsqldb
* [<a href="https://issues.apache.org/jira/browse/CALCITE-376">CALCITE-376</a>]
  Rename from SqlRun to Quidem and factor out of Calcite as separate project
* Fix connection leak
* Carry on after certain errors (e.g. `AssertionError`)
* Add '`if (true)`' command
* Allow '`!plan`' after '`!ok`' for same SQL statement
* Add '`!plan`' command
* [<a href="https://issues.apache.org/jira/browse/CALCITE-318">CALCITE-318</a>]
  Add unit test for SqlRun
* Match output regardless of order if `ORDER BY` not present
* Add '`!skip`' command
* Add MySQL formatting mode
* Add `SqlRun`, an idempotent utility for running SQL test scripts
