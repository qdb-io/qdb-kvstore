package io.qdb.kvstore

import spock.lang.Specification
import spock.lang.Shared
import spock.lang.Stepwise
import org.apache.commons.io.FileUtils
import java.util.concurrent.ConcurrentMap

/**
 * Basic non-clustered tests.
 */
@Stepwise
class BasicKvStoreSpec extends Specification {

    @Shared KeyValueStore<Integer, ModelObject> store
    @Shared ConcurrentMap<Integer, ModelObject> widgets

    def setupSpec() {
        def dir = new File("build/test-basic")
        if (dir.exists() && dir.isDirectory()) FileUtils.deleteDirectory(dir)
        store = new KeyValueStoreBuilder<Integer, ModelObject>()
                .dir(dir)
                .serializer(new JsonSerializer())
                .versionProvider(new VersionProvider())
                .create()
        widgets = store.getMap("widgets")
    }

    def "isEmpty"() {
        expect:
        store.isEmpty()
    }

    def "put and get"() {
        def put = widgets.put(1, new ModelObject("one"))
        def get = widgets.get(1)

        expect:
        !store.isEmpty()
        put == null
        get.name == "one"
        get.version == 1
    }

    def "opt lock check on put"() {
        when:
        widgets.put(1, new ModelObject("one"))

        then:
        thrown(OptimisticLockingException)
    }

    def "putIfAbsent"() {
        def ans1 = widgets.putIfAbsent(1, new ModelObject("onexx"))
        def get1 = widgets.get(1)
        def ans2 = widgets.putIfAbsent(2, new ModelObject("two"))
        def get2 = widgets.get(2)

        expect:
        ans1.name == "one"
        get1.name == "one"
        ans2 == null
        get2.name == "two"
        get2.version == 1
    }

    def "replace"() {
        def ans1 = widgets.replace(1, new ModelObject("onexx", 1))
        def get1 = widgets.get(1)
        def ans3 = widgets.replace(3, new ModelObject("three"))
        def get3 = widgets.get(3)

        expect:
        ans1.name == "one"
        get1.name == "onexx"
        get1.version == 2
        ans3 == null
        get3 == null
    }

    def "replace oldValue newValue"() {
        def ans0 = widgets.replace(1, new ModelObject("onex"), new ModelObject("one"))
        def ans1 = widgets.replace(1, new ModelObject("onexx"), new ModelObject("one"))
        def get1 = widgets.get(1)

        expect:
        !ans0
        ans1
        get1.name == "one"
        get1.version == 1
    }

}
