package io.qdb.kvstore.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Sends messages to servers in our cluster. Messages received from other servers must be posted on the shared
 * event bus as {@link MessageReceived} events.
 */
public interface Transport {

    /**
     * Get our server.
     */
    String getSelf();

    /**
     * Send a message to the server asynchronously.
     */
    void send(String to, Message msg);

    /**
     * Read the latest snapshot the server has.
     */
    InputStream getLatestSnapshotFrom(String from) throws IOException;

    /**
     * Stream transactions since txId from the server.
     */
    StoreTxAndId.Iter getTransactionsFrom(String from, long txId) throws IOException;

}
