package io.qdb.kvstore

import org.apache.commons.io.FileUtils
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise
import io.qdb.kvstore.cluster.ClusteredKeyValueStore

/**
 * Saving and loading snapshots.
 */
@Stepwise
class SnapshotSpec extends Specification {

    @Shared File baseDir = new File("build/test-snapshots")
    @Shared FilenameFilter filter = new RegexFilenameFilter(".+\\.snapshot")

    private ClusteredKeyValueStore<String, ModelObject> createStore(File dir, boolean nuke = true) {
        if (nuke && dir.exists() && dir.isDirectory()) FileUtils.deleteDirectory(dir)
        return new KeyValueStoreBuilder<Integer, ModelObject>()
                .dir(dir)
                .serializer(new JsonSerializer())
                .versionProvider(new VersionProvider())
                .create() as ClusteredKeyValueStore<String, ModelObject>
    }

    def "saveSnapshot with no changes is NOP"() {
        File dir = new File(baseDir, "one")
        def store = createStore(dir)
        store.saveSnapshot()
        store.close()

        expect:
        dir.list(filter).length == 0
    }

    def "saveSnapshot"() {
        File dir = new File(baseDir, "one")
        def store = createStore(dir)
        store.getMap("widgets").put("1", new ModelObject("one"))
        store.saveSnapshot()
        store.close()

        expect:
        dir.list(filter).length == 1
    }

    def "loadSnapshot on startup"() {
        def store = createStore(new File(baseDir, "one"), false)
        def widgets = store.getMap("widgets")
        def sz = widgets.size()
        def one = widgets.get("1")
        store.close()

        expect:
        sz == 1
        one instanceof ModelObject
        one.name == "one"
        one.version == 1
    }

    def "transfer snapshot"() {
        def store = createStore(new File(baseDir, "one"), false)
        def store2 = createStore(new File(baseDir, "two"), true)

        def snapshot = store.createSnapshot()
        store2.loadSnapshot(snapshot)

        IllegalStateException exception = null
        try {
            store2.loadSnapshot(snapshot)
        } catch (IllegalStateException e) {
            exception = e;
        }

        def widgets = store2.getMap("widgets")
        def sz = widgets.size()
        def one = widgets.get("1")
        store.close()
        store2.close()

        expect:
        sz == 1
        one instanceof ModelObject
        one.name == "one"
        one.version == 1
        exception != null
    }

    def "replay tx log on startup"() {
        File dir = new File(baseDir, "three")
        def store = createStore(dir)
        store.getMap("widgets").put("1", new ModelObject("one"))
        store.close()

        store = createStore(dir, false)
        def widgets = store.getMap("widgets")
        def sz = widgets.size()
        def one = widgets.get("1")
        store.close()

        expect:
        sz == 1
        one instanceof ModelObject
        one.name == "one"
        one.version == 1
    }


}
