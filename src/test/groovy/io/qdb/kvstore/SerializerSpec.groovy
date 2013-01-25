package io.qdb.kvstore

import spock.lang.Specification

/**
 * 
 */
class SerializerSpec extends Specification {

    def "Serialize/deserialize tx"() {
        StoreTx tx = new StoreTx()
        tx.namespace = "widgets"
        tx.op = StoreTx.Operation.PUT
        tx.key = 1
        tx.value = new ModelObject("one");

        def ser = new JsonSerializer()

        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        ser.serialize(tx, bos)
        bos.close()
        println(new String(bos.toByteArray(), "UTF8"));

        ByteArrayInputStream ins = new ByteArrayInputStream(bos.toByteArray());
        def tx2 = ser.deserialize(ins, StoreTx.class)
        println(tx2.value)

        expect:
        true
    }

    def "Serialize/deserialize snapshot"() {
        KeyValueStore.Snapshot snapshot = new KeyValueStore.Snapshot()
        snapshot.maps = new HashMap();
        def widgets = new HashMap()
        snapshot.maps.put("widgets", widgets)
        widgets.put(1, new ModelObject("one"))
        print(snapshot)

        def ser = new JsonSerializer()

        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        ser.serialize(snapshot, bos)
        bos.close()
        println(new String(bos.toByteArray(), "UTF8"));

        ByteArrayInputStream ins = new ByteArrayInputStream(bos.toByteArray());
        def s2 = ser.deserialize(ins, KeyValueStore.Snapshot)
        print(s2)

        expect:
        true
    }

    private print(KeyValueStore.Snapshot tx2) {
        tx2.maps.each { k, v ->
            println("" + k + " = " + v.class + " " + v);
            v.each { k2, v2 ->
                println("" + k2.class + " " + k2 + " = " + v2.class + " " + v2);
            }
        }
    }

}
