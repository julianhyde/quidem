<!--
{% comment %}
Licensed to Julian Hyde under one or more contributor license
agreements.  See the NOTICE file distributed with this work
for additional information regarding copyright ownership.
Julian Hyde licenses this file to you under the Apache
License, Version 2.0 (the "License"); you may not use this
file except in compliance with the License.  You may obtain a
copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied.  See the License for the specific
language governing permissions and limitations under the
License.
{% endcomment %}
-->

# Quidem HOWTO

Here's some miscellaneous documentation about using and developing Quidem.

# Release

Make sure that `./mvnw clean install site` runs on JDK 11, 17 and 21
on Linux, macOS and Windows.

Upgrade dependencies to their latest release: run
```bash
./mvnw versions:update-properties
```
and commit the modified `pom.xml`.

Update the [release history](HISTORY.md),
the version number at the bottom of [README](README.md),
and the copyright date in [NOTICE](NOTICE).

```
./mvnw clean
./mvnw release:clean
git clean -nx
git clean -fx
read -s GPG_PASSPHRASE
./mvnw -Prelease -Dgpg.passphrase=${GPG_PASSPHRASE} release:prepare
./mvnw -Prelease -Dgpg.passphrase=${GPG_PASSPHRASE} release:perform
```

Then go to [Sonatype](https://oss.sonatype.org/#stagingRepositories),
log in, close the repository, and release.

Make sure that the [site](http://www.hydromatic.net/quidem/) has been updated.

[Announce the release](https://twitter.com/julianhyde/status/622842100736856064).
