package io.qdb.kvstore.cluster

import spock.lang.Specification
import spock.lang.Stepwise
import io.qdb.kvstore.JsonSerializer
import io.qdb.kvstore.KeyValueStore
import java.util.zip.GZIPOutputStream
import io.qdb.buffer.MessageCursor
import io.qdb.kvstore.StoreTx

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

    def "Empty message received ignored"() {
        Protocol p = new Protocol(transport, serializer, listener)
        p.setStore(store)
        p.onMessageReceived("s1", new ByteArrayInputStream(new byte [0]), null)

        expect:
        true
    }

    def "Bad message received ignored"() {
        Protocol p = new Protocol(transport, serializer, listener)
        p.setStore(store)
        p.onMessageReceived("s1", new ByteArrayInputStream([42] as byte[]), null)

        expect:
        true
    }

    def "Send and receive Paxos message"() {
        def pm = new PaxosMessage(Paxos.Msg.Type.PREPARE, null, null)

        Protocol p = new Protocol(transport, serializer, listener)
        p.setStore(store)
        p.send("s2", pm, "s1")
        p.onMessageReceived("s1", new ByteArrayInputStream(transport.msg), null)

        expect:
        transport.to == "s2"
        transport.msg[0] == 1   // TYPE_PAXOS_MSG
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
        transport.msg == [2] as byte[]  // TYPE_GET_SNAPSHOT
        bos.toByteArray() == zippedSnapshot
    }

    def "Get and receive transactions"() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        DataOutputStream dos = new DataOutputStream(bos)
        dos.write(3)        // TYPE_GET_TRANSACTIONS
        dos.writeLong(123L)
        dos.close()
        byte[] expectedMsg = bos.toByteArray()

        MessageCursorImpl c = new MessageCursorImpl(nextId: 123)
        c.add(serialize(new StoreTx("a", StoreTx.Operation.NOP, null)))
        c.add(serialize(new StoreTx("b", StoreTx.Operation.NOP, null)))
        store.openTxLogCursor(_) >> c

        Protocol p = new Protocol(transport, serializer, listener)
        p.setStore(store)
        p.onMessageReceived("s1", new ByteArrayInputStream(expectedMsg), bos = new ByteArrayOutputStream())

        transport.response = bos.toByteArray()
        List<Protocol.Tx> list = p.getTransactions("s2", 123L).toList()

        expect:
        transport.response.length > 0
        transport.msg == expectedMsg
        list.size() == 2
        list[0].id == 123
    }

    private byte[] serialize(Object o) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        serializer.serialize(o, bos)
        return bos.toByteArray()
    }

    static class Payload {
        long id
        byte[] data
    }

    static class MessageCursorImpl implements MessageCursor {

        List<Payload> list = []
        int pos = -1
        int nextId

        void add(byte[] data) {
            list << new Payload(id: nextId, data: data)
            nextId += data.length
        }

        boolean next() { return ++pos < list.size() }
        long getId() { return list[pos].id }
        int getPayloadSize() { return list[pos].data.length }
        byte[] getPayload() { return list[pos].data }
        boolean next(int timeoutMs) { return false }
        long getTimestamp() { return 0 }
        String getRoutingKey() { return null }
        void close() { }
    }
}
