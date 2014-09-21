# Quidem release history

For a full list of releases, see <a href="https://github.com/julianhyde/quidem/releases">github</a>.

## <a href="https://github.com/julianhyde/quidem/releases/tag/quidem-0.1">0.1</a> / 2014-09-21

* Convert test suite to hsqldb
* Rename from SqlRun to Quidem and factor out of Optiq as separate project
* Fix connection leak
* Carry on after certain errors (e.g. AssertionError)
* Add 'if (true)' command
* Allow '!plan' after '!ok' for same SQL statement
* Add '!plan' command
* [OPTIQ-318] Add unit test for SqlRun
* Match output regardless of order if ORDER BY not present
* Add "!skip" command
* Add MySQL formatting mode
* Add SqlRun, an idempotent utility for running SQL test scripts
