package io.qdb.kvstore.cluster;

import io.qdb.kvstore.StoreTx;

import java.io.IOException;
import java.io.InputStream;

/**
 * Sends messages to servers in our cluster.
 */
public interface Transport {

    /**
     * Get our server.
     */
    String getSelf();

    /**
     * Send a message to the server asynchronously.
     */
    void send(String to, Paxos.Msg<SequenceNo, StoreTx> msg);

    /**
     * Read the latest snapshot the server has.
     */
    InputStream getLatestSnapshotFrom(String from) throws IOException;

    /**
     * Stream transactions since txId from the server.
     */
    StoreTxIterator getTransactionsFrom(String from, long txId) throws IOException;

    public interface StoreTxIterator {
        /** Get the next transaction or null if there are no more. */
        StoreTxAndId next() throws IOException;
    }

    public static class StoreTxAndId {

        public final long txId;
        public final StoreTx storeTx;

        public StoreTxAndId(long txId, StoreTx storeTx) {
            this.txId = txId;
            this.storeTx = storeTx;
        }
    }
}
