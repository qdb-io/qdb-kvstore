qdb-kvstore
===========

Clustered in memory key/value store for objects. Guarantee's that all nodes in the cluster will see the same data.
Writes the database to disk periodically in snapshot files. Uses a MessageBuffer as a tx log for replay from the
last snapshot after a crash. The default serializer stores objects as JSON so the snapshot files and so on are
human readable.

Fire's events when objects are created, updated or deleted.


Changelog
---------

0.1.0:
- Initial release


Building
--------

This project is built using Gradle (http://www.gradle.org/). Download and install Gradle (just unzip it and
make sure 'gradle' is on your path). Then do:

    $ gradle check
    $ gradle assemble

This will run the unit tests and create jars in build/libs.


License
-------

Copyright 2012 David Tinker

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
