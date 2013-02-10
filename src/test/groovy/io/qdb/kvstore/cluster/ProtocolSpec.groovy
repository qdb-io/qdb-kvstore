package io.qdb.kvstore.cluster

import spock.lang.Specification
import spock.lang.Stepwise
import io.qdb.kvstore.JsonSerializer
import io.qdb.kvstore.KeyValueStore
import java.util.zip.GZIPOutputStream

/**
 * Tests for encoding and decoding of messages.
 */
@SuppressWarnings("GroovyAssignabilityCheck")
@Stepwise
class ProtocolSpec extends Specification {

    TransportForTests transport = new TransportForTests("s1")
    JsonSerializer serializer = new JsonSerializer()
    ProtocolListenerForTests listener = new ProtocolListenerForTests()
    ClusteredKeyValueStore store = Mock()

    def "Cannot receive messages until store set"() {
        when:
        new Protocol(transport, serializer, listener).onMessageReceived("s1", null, null)

        then:
        thrown(IllegalStateException)
    }

    def "Send and receive Paxos message"() {
        def pm = new PaxosMessage(Paxos.Msg.Type.PREPARE, null, null)

        Protocol p = new Protocol(transport, serializer, listener)
        p.setStore(store)
        p.send("s2", pm, "s1")
        p.onMessageReceived("s1", new ByteArrayInputStream(transport.msg), null)

        expect:
        transport.to == "s2"
        listener.msg.type == Paxos.Msg.Type.PREPARE
    }

    def "Get and receive snapshot"() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        GZIPOutputStream out = new GZIPOutputStream(bos)
        def ts = new KeyValueStore.Snapshot(txId: 123L, storeId: "abc", maps: [:])
        serializer.serialize(ts, out)
        out.close()
        def zippedSnapshot = bos.toByteArray()

        Protocol p = new Protocol(transport, serializer, listener)
        p.setStore(store)
        transport.response = zippedSnapshot
        def snapshot = p.getSnapshot("s2")

        bos = new ByteArrayOutputStream()
        store.createSnapshot() >> ts
        p.onMessageReceived("s1", new ByteArrayInputStream(transport.msg), bos)

        expect:
        transport.from == "s2"
        snapshot.txId == 123L
        bos.toByteArray() == zippedSnapshot
    }

//    def "Get and receive transactions"() {
//        ByteArrayOutputStream bos = new ByteArrayOutputStream()
//        GZIPOutputStream out = new GZIPOutputStream(bos)
//        def ts = new KeyValueStore.Snapshot(txId: 123L, storeId: "abc", maps: [:])
//        serializer.serialize(ts, out)
//        out.close()
//        def zippedSnapshot = bos.toByteArray()
//
//        store.createSnapshot() >> ts
//
//        Protocol p = new Protocol(transport, serializer, listener)
//        p.setStore(store)
//        transport.response = zippedSnapshot
//        def i = p.getTransactions("s2", 123L)
//
//        bos = new ByteArrayOutputStream()
//        p.onMessageReceived("s1", new ByteArrayInputStream(transport.msg), bos)
//
//        expect:
//        transport.from == "s2"
//        snapshot.txId == 123L
//        bos.toByteArray() == zippedSnapshot
//    }
}
