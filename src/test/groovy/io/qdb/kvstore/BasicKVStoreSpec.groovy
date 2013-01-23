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
    @Shared ConcurrentMap<Integer, ModelObject> bugs

    def setupSpec() {
        def dir = new File("build/test-basic")
        if (dir.exists() && dir.isDirectory()) FileUtils.deleteDirectory(dir)
        store = new KeyValueStoreBuilder<Integer, ModelObject>()
                .dir(dir)
                .serializer(new JsonSerializer())
                .versionProvider(new VersionProvider())
                .create()
        widgets = store.getMap("widgets")
        bugs = store.getMap("bugs")
    }

    def "isEmpty"() {
        expect:
        store.isEmpty()
        widgets.isEmpty()
        widgets.size() == 0
    }

    def "put and get"() {
        def put = widgets.put(1, new ModelObject("one"))
        def get = widgets.get(1)
        def none = bugs.get(1)

        expect:
        !store.isEmpty()
        !widgets.isEmpty()
        widgets.size() == 1
        put == null
        get.name == "one"
        get.version == 1
        none == null
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

    def "remove"() {
        def ans = widgets.remove(2)
        def get = widgets.get(2)

        expect:
        ans.name == "two"
        get == null
    }

    def "remove value opt lock check"() {
        when:
        widgets.put(2, new ModelObject("two", 22))
        widgets.remove(2, new ModelObject("two"))

        then:
        thrown(OptimisticLockingException)
    }

    def "remove value"() {
        def ans0 = widgets.remove(2, new ModelObject("twox", 23))
        def ans2 = widgets.remove(2, new ModelObject("two", 23))
        def get = widgets.get(2)

        expect:
        !ans0
        ans2
        get == null
    }

    def "keySet"() {
        def keys = widgets.keySet().asList()
        def none = bugs.keySet()

        expect:
        keys == [1]
        none.isEmpty()
    }

    def "values"() {
        def values = widgets.values().asList()
        def none = bugs.values()

        expect:
        values.size() == 1
        values[0].name == "one"
        none.isEmpty()
    }

    def "entrySet"() {
        def es = widgets.entrySet().asList()
        def none = bugs.entrySet()

        expect:
        es.size() == 1
        es[0].key == 1
        es[0].value.name == "one"
        none.isEmpty()
    }

    def "size"() {
        def sz = widgets.size()
        def none = bugs.size()

        expect:
        sz == 1
        none == 0
    }

    def "containsKey"() {
        def one = widgets.containsKey(1)
        def two = widgets.containsKey(2)
        def none = bugs.containsKey(1)

        expect:
        one
        !two
        !none
    }

    def "containsValue"() {
        def one = widgets.containsValue(new ModelObject("one"))
        def two = widgets.containsValue(new ModelObject("two"))
        def none = bugs.containsValue(new ModelObject("two"))

        expect:
        one
        !two
        !none
    }

    def "clear"() {
        widgets.clear()
        widgets.clear()     // just to cover the null map condition

        expect:
        widgets.isEmpty()
    }

    def "putAll"() {
        Map<Integer, ModelObject> map = [:]
        map.put(1, new ModelObject("one"))
        widgets.putAll(map)

        expect:
        widgets.size() == 1
        widgets.values().iterator().next().name == "one"
    }

}