package io.qdb.kvstore.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Sends messages to servers in our cluster.
 */
public interface Transport {

    public interface MessageListener {
        /**
         * Accept a message from another server in the cluster. Write a response to out.
         */
        void onMessageReceived(String from, InputStream ins, OutputStream out) throws IOException;
    }

    /**
     * No calls will be made to the transport until init has been called. Messages received from other servers
     * in the cluster should be delivered to the listener's onMessageReceived method.
     */
    void init(MessageListener listener);

    /**
     * Get our server.
     */
    String getSelf();

    /**
     * Send a message to the server asynchronously.
     */
    void send(String to, byte[] msg);

    /**
     * Send a message to the server synchronously and return the response.
     */
    byte[] get(String from, byte[] msg) throws IOException;

}
