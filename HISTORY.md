# Quidem release history

For a full list of releases, see <a href="https://github.com/julianhyde/quidem/releases">github</a>.

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
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-376">OPTIQ-376</a>]
  Rename from SqlRun to Quidem and factor out of Optiq as separate project
* Fix connection leak
* Carry on after certain errors (e.g. `AssertionError`)
* Add '`if (true)`' command
* Allow '`!plan`' after '`!ok`' for same SQL statement
* Add '`!plan`' command
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-318">OPTIQ-318</a>]
  Add unit test for SqlRun
* Match output regardless of order if `ORDER BY` not present
* Add '`!skip`' command
* Add MySQL formatting mode
* Add `SqlRun`, an idempotent utility for running SQL test scripts
