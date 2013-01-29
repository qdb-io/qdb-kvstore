package io.qdb.kvstore.cluster;

/**
 * Sends messages to servers in our cluster.
 */
public interface Transport {

    /**
     * Get our server.
     */
    String getSelf();

    /**
     * Send a message to another server.
     */
    void send(Message msg, String to);

}
