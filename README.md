qdb-kvstore
===========

Transactional in memory key/value store for objects. Writes the database to disk periodically in snapshot files.
Uses a MessageBuffer as a tx log for replay from the last snapshot after a crash. Clustering is optional and
all nodes in the cluster see the same view of the store.

Fire's events when objects are created, updated or deleted.


Usage
-----

Create a KeyValueStore using a KeyValueStoreBuilder:

    KeyValueStore<Integer, ModelObject> store = new KeyValueStoreBuilder<Integer, ModelObject>()
        .dir(dir)
        .serializer(new JsonSerializer())
        .versionProvider(new VersionProvider())
        .create();

In this case the keys are Integer's and the values are ModelObject's.

All parameters except dir and serializer are optional. The serializer is responsible for writing and reading objects
to/from streams. The store keeps its snapshots and transaction log in dir. You need to supply a version provider
if you want to use optimistic locking.

Once you have a store you can get named maps and use them as you would a normal java.util.ConcurrentMap. However
all map methods might throw a KeyValueStoreException and all writes to the store are serialized. In addition if
you are using optimistic locking then replacing one value with another will only work if the incoming value has
the same version number as the existing value. If not an OptimisticLockingException is thrown.

    ConcurrentMap<Integer, ModelObject> widgets = store.getMap("widgets");
    widgets.put(1, new ModelObject("A widget"));
    ...
    ModelObject foobar = widgets.get(1);


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
