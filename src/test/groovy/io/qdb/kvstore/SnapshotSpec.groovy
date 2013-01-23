package io.qdb.kvstore

import org.apache.commons.io.FileUtils
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise

import java.util.concurrent.ConcurrentMap
import com.google.common.io.PatternFilenameFilter

/**
 * Saving and loading snapshots.
 */
@Stepwise
class SnapshotSpec extends Specification {

    @Shared File dir = new File("build/test-snapshots")
    @Shared KeyValueStore<Integer, ModelObject> store
    @Shared ConcurrentMap<Integer, ModelObject> widgets
    @Shared FilenameFilter filter = new PatternFilenameFilter(".+\\.snapshot")

    def setupSpec() {
        if (dir.exists() && dir.isDirectory()) FileUtils.deleteDirectory(dir)
        store = createStore(dir)
        widgets = store.getMap("widgets")
    }

    private KeyValueStore<Integer, ModelObject> createStore(File dir) {
        return new KeyValueStoreBuilder<Integer, ModelObject>()
                .dir(dir)
                .serializer(new JsonSerializer())
                .versionProvider(new VersionProvider())
                .create()
    }

    def "saveSnapshot with no changes is NOP"() {
        store.saveSnapshot()

        expect:
        dir.list(filter).length == 0
    }

    def "saveSnapshot"() {
        widgets.put(1, new ModelObject("one"))
        store.saveSnapshot()

        expect:
        dir.list(filter).length == 1
    }

    def "loadSnapshot"() {
        store.close()
        store = createStore(dir)
        widgets = store.getMap("widgets")

        expect:
        widgets.size() == 1
        widgets.get(1).name == "one"
    }
}
