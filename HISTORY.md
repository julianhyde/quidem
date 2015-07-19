# Quidem release history

For a full list of releases, see <a href="https://github.com/julianhyde/quidem/releases">github</a>.

## <a href="https://github.com/julianhyde/quidem/releases/tag/quidem-0.6">0.6</a> / 2015-07-19

* Add '--var' argument, so you can pass variables from the command line
* [<a href="https://github.com/julianhyde/quidem/issues/7">QUIDEM-7</a>]
  Don't be fooled by ORDER BY inside windowed aggregate
* [<a href="https://github.com/julianhyde/quidem/issues/8">QUIDEM-8</a>]
  [QUIDEM-8] Allow variables in 'if'
* Truncate error stack if it exceeds N characters
* Add maven central

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
