package io.qdb.kvstore.cluster;

import io.qdb.buffer.MessageCursor;
import io.qdb.kvstore.KeyValueStore;
import io.qdb.kvstore.StoreTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Encodes and decodes the messages we send to and receive from other servers in a cluster.
 */
public class Protocol implements Transport.MessageListener, Paxos.Transport<SequenceNo, StoreTx> {

    private static final Logger log = LoggerFactory.getLogger(Protocol.class);

    private final Transport transport;
    private final KeyValueStore.Serializer serializer;
    private final Listener listener;

    private ClusteredKeyValueStore store;

    private static final int TYPE_PAXOS_MSG = 1;
    private static final int TYPE_GET_SNAPSHOT = 2;
    private static final int TYPE_GET_TRANSACTIONS = 3;

    private static final byte[] GET_SNAPSHOT_MSG = new byte[]{TYPE_GET_SNAPSHOT};

    public interface Listener {
        void onPaxosMessageReceived(String from, PaxosMessage msg);
    }

    public Protocol(Transport transport, KeyValueStore.Serializer serializer, Listener listener) {
        this.transport = transport;
        this.serializer = serializer;
        this.listener = listener;
        transport.init(this);
    }

    public void setStore(ClusteredKeyValueStore store) {
        this.store = store;
    }

    @Override
    public void send(Object to, Paxos.Msg<SequenceNo, StoreTx> msg, Object from) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_PAXOS_MSG);
        try {
            serializer.serialize(msg, bos);
            transport.send((String) to, bos.toByteArray());
        } catch (IOException e) {
            log.error(e.toString(), e);
        }
    }

    private void handlePaxosMsg(String from, InputStream ins) throws IOException {
        listener.onPaxosMessageReceived(from, serializer.deserialize(ins, PaxosMessage.class));
    }

    /**
     * Get a snapshot from another server.
     */
    public KeyValueStore.Snapshot getSnapshot(String from) throws IOException {
        GZIPInputStream zin = new GZIPInputStream(new ByteArrayInputStream(transport.get(from, GET_SNAPSHOT_MSG)));
        try {
            return serializer.deserialize(zin, KeyValueStore.Snapshot.class);
        } finally {
            close(zin);
        }
    }

    private void handleGetSnapshot(OutputStream out) throws IOException {
        KeyValueStore.Snapshot snapshot = store.createSnapshot();
        GZIPOutputStream zout = new GZIPOutputStream(out);
        serializer.serialize(snapshot, zout);
        zout.close();
    }

    /**
     * Get a batch of transactions from another server. Throws IllegalArgumentException if the server does not
     * have transactions as far back as fromTxId.
     */
    public Iterator<Tx> getTransactions(String from, long fromTxId) throws IOException, IllegalArgumentException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        out.write(TYPE_GET_TRANSACTIONS);
        out.writeLong(fromTxId);
        out.close();

        byte[] zipped = transport.get(from, bos.toByteArray());

        final DataInputStream din = new DataInputStream(new GZIPInputStream(new ByteArrayInputStream(zipped)));

        if (!din.readBoolean()) {
            throw new IllegalArgumentException("Requested txId (0x" + Long.toHexString(fromTxId) + ") is too old");
        }

        return new Iterator<Tx>() {

            private Tx buf;

            @Override
            public boolean hasNext() {
                try {
                    long txId;
                    try {
                        txId = din.readLong();
                    } catch (EOFException e) {
                        return false;
                    }
                    int sz = din.readInt();
                    byte[] payload = new byte[sz];
                    din.readFully(payload);
                    buf = new Tx(txId, serializer.deserialize(new ByteArrayInputStream(payload), StoreTx.class));
                    return true;
                } catch (IOException e) {
                    throw new IllegalStateException(e.toString(), e);
                }
            }

            @Override
            public Tx next() {
                if (buf == null) throw new NoSuchElementException();
                Tx ans = buf;
                buf = null;
                return ans;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private void handleGetTransactions(InputStream ins, OutputStream out) throws IOException {
        DataInputStream din = new DataInputStream(ins);
        long fromTxId = din.readLong();

        DataOutputStream dos = new DataOutputStream(new GZIPOutputStream(out));

        MessageCursor c = store.openTxLogCursor(fromTxId);
        try {
            boolean first = true;
            for (; c.next(); ) {
                long id = c.getId();
                if (first) {
                    boolean badTxId = id != fromTxId;
                    dos.writeBoolean(badTxId);
                    if (badTxId) break;
                    first = false;
                }
                dos.writeLong(id);
                byte[] payload = c.getPayload();
                dos.writeInt(payload.length);
                dos.write(payload);
            }
            dos.flush();
        } finally {
            try {
                c.close();
            } catch (IOException e) {
                log.error("Error closing cursor: " + e, e);
            }
        }
    }

    @Override
    public void onMessageReceived(String from, InputStream ins, OutputStream out) throws IOException {
        if (store == null) throw new IllegalStateException("Not ready to receive messages as store has not been set");
        int type = ins.read();
        switch (type) {
            case -1:
                log.error("Empty message received from " + from);
                break;
            case TYPE_PAXOS_MSG:
                handlePaxosMsg(from, ins);
                break;
            case TYPE_GET_SNAPSHOT:
                handleGetSnapshot(out);
                break;
            case TYPE_GET_TRANSACTIONS:
                handleGetTransactions(ins, out);
                break;
            default:
                log.error("Unknown message type " + type + " (0x" + Integer.toHexString(type & 0xFF) +
                        ") received from " + from);
        }
    }

    private void close(Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                if (log.isDebugEnabled()) log.debug(e.toString(), e);
            }
        }
    }

    public static class Tx {

        public final long id;
        public final StoreTx storeTx;

        public Tx(long id, StoreTx storeTx) {
            this.id = id;
            this.storeTx = storeTx;
        }
    }

}
